package com.volo.broker.transport.grpc.flow;

import com.custombroker.grpc.PublisherResponse;
import com.volo.broker.broker.flow.FlowTransition;
import com.volo.broker.broker.flow.GlobalMemoryGuard;
import com.volo.broker.broker.flow.QueueFlowController;
import com.volo.broker.config.FlowControlProperties;
import com.volo.broker.transport.grpc.SafeGrpcObserver;
import com.volo.broker.transport.grpc.connection.PublisherConnection;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class PublisherFlowGateTest {

    private ServerCallStreamObserver<PublisherResponse> serverCall;
    private QueueFlowController flowController;
    private PublisherConnection connection;
    private StreamObserver<PublisherResponse> responseObserver;
    private PublisherFlowGate gate;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        serverCall = mock(ServerCallStreamObserver.class);
        responseObserver = mock(StreamObserver.class);
        FlowControlProperties props = new FlowControlProperties(true, null, null, 16);
        GlobalMemoryGuard guard = new GlobalMemoryGuard(props);
        flowController = new QueueFlowController(guard, props);

        connection = new PublisherConnection(new SafeGrpcObserver<>(responseObserver));
        gate = new PublisherFlowGate(serverCall, flowController, connection);
        connection.attachFlowGate(gate);
    }

    @Test
    @DisplayName("afterRequestProcessed calls request(1) when queues are open")
    void afterRequestProcessed_openQueues_requestsNext() {
        gate.afterRequestProcessed(List.of("q1", "q2"));

        verify(serverCall, times(1)).request(1);
        assertThat(gate.isPaused()).isFalse();
        assertThat(gate.state()).isEqualTo(PublisherFlowGate.GateState.FLOWING);
    }

    @Test
    @DisplayName("afterRequestProcessed pauses gate and sends notice when any queue is blocked")
    void afterRequestProcessed_blockedQueue_pauses() {
        flowController.handleTransition("q1", FlowTransition.TO_BLOCKED);

        gate.afterRequestProcessed(List.of("q1", "q2"));

        // Must NOT request next frame
        verify(serverCall, never()).request(anyInt());
        assertThat(gate.isPaused()).isTrue();
        assertThat(gate.state()).isEqualTo(PublisherFlowGate.GateState.PAUSED);

        // Verify FlowControlNotice(blocked=true) was sent
        ArgumentCaptor<PublisherResponse> captor = ArgumentCaptor.forClass(PublisherResponse.class);
        verify(responseObserver).onNext(captor.capture());
        assertThat(captor.getValue().hasFlow()).isTrue();
        assertThat(captor.getValue().getFlow().getBlocked()).isTrue();
    }

    @Test
    @DisplayName("resume restores FLOWING state and calls request(1)")
    void resume_restoresFlowing() {
        flowController.handleTransition("q1", FlowTransition.TO_BLOCKED);
        gate.afterRequestProcessed(List.of("q1"));
        assertThat(gate.isPaused()).isTrue();

        reset(serverCall, responseObserver);

        gate.resume();

        verify(serverCall, times(1)).request(1);
        assertThat(gate.isPaused()).isFalse();
        assertThat(gate.state()).isEqualTo(PublisherFlowGate.GateState.FLOWING);

        ArgumentCaptor<PublisherResponse> captor = ArgumentCaptor.forClass(PublisherResponse.class);
        verify(responseObserver).onNext(captor.capture());
        assertThat(captor.getValue().hasFlow()).isTrue();
        assertThat(captor.getValue().getFlow().getBlocked()).isFalse();
    }

    @Test
    @DisplayName("close marks state CLOSED and unregisters from controller")
    void close_marksClosed() {
        gate.close();

        assertThat(gate.state()).isEqualTo(PublisherFlowGate.GateState.CLOSED);
        gate.afterRequestProcessed(List.of("q1"));
        verify(serverCall, never()).request(anyInt());
    }
}

