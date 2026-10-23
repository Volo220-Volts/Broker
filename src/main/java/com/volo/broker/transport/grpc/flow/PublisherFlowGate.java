package com.volo.broker.transport.grpc.flow;

import com.volo.broker.broker.flow.FlowGate;
import com.volo.broker.broker.flow.QueueFlowController;
import com.volo.broker.transport.grpc.connection.PublisherConnection;
import com.volo.broker.transport.grpc.mapper.GrpcMessageConverters;
import io.grpc.stub.ServerCallStreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PublisherFlowGate implements FlowGate {

    private static final Logger log = LoggerFactory.getLogger(PublisherFlowGate.class);

    public enum GateState { FLOWING, PAUSED, CLOSED }

    private final ServerCallStreamObserver<?> serverObserver;
    private final QueueFlowController flowController;
    private volatile PublisherConnection connection;
    private final AtomicReference<GateState> state = new AtomicReference<>(GateState.FLOWING);
    private final AtomicLong pausedAtTimestamp = new AtomicLong(0);
    private final AtomicBoolean overdue = new AtomicBoolean(false);

    public PublisherFlowGate(
            ServerCallStreamObserver<?> serverObserver,
            QueueFlowController flowController,
            PublisherConnection connection
    ) {
        this.serverObserver = serverObserver;
        this.flowController = flowController;
        this.connection = connection;
    }

    public void setConnection(PublisherConnection connection) {
        this.connection = connection;
    }

    public void afterRequestProcessed(Collection<String> routedQueues) {
        if (state.get() == GateState.CLOSED) {
            return;
        }

        if (flowController != null && flowController.isAnyBlocked(routedQueues)) {
            if (state.compareAndSet(GateState.FLOWING, GateState.PAUSED)) {
                pausedAtTimestamp.set(System.currentTimeMillis());
                log.debug("Pausing publisher flow gate for queues: {}", routedQueues);
                if (connection != null) {
                    connection.send(GrpcMessageConverters.toFlowControlNoticeResponse(
                            true,
                            "QUEUE_WATERMARK:" + (routedQueues != null ? routedQueues : "[]")
                    ));
                }
                flowController.park(this, routedQueues);
                return;
            }
        }

        if (state.get() == GateState.FLOWING) {
            requestNext();
        }
    }

    @Override
    public void resume() {
        if (state.compareAndSet(GateState.PAUSED, GateState.FLOWING)) {
            pausedAtTimestamp.set(0);
            log.debug("Resuming publisher flow gate");
            if (connection != null) {
                connection.send(GrpcMessageConverters.toFlowControlNoticeResponse(false, "OPEN"));
            }
            requestNext();
        }
    }

    @Override
    public void notifyOverdue() {
        if (overdue.compareAndSet(false, true)) {
            log.warn("Publisher flow gate timed out waiting for queue unblock (OVERDUE)");
            resume();
        }
    }

    @Override
    public void onFlowNotice(boolean blocked, String reason) {
        if (connection != null) {
            connection.send(GrpcMessageConverters.toFlowControlNoticeResponse(blocked, reason));
        }
    }

    public void close() {
        state.set(GateState.CLOSED);
        if (flowController != null) {
            flowController.unregister(this);
        }
    }

    public boolean isPaused() {
        return state.get() == GateState.PAUSED;
    }

    public boolean isOverdue() {
        return overdue.get();
    }

    public void clearOverdue() {
        overdue.set(false);
    }

    @Override
    public long pausedAtTimestamp() {
        return pausedAtTimestamp.get();
    }

    public GateState state() {
        return state.get();
    }

    private void requestNext() {
        if (serverObserver != null) {
            try {
                serverObserver.request(1);
            } catch (Throwable t) {
                log.warn("Failed to request next gRPC frame: {}", t.getMessage());
            }
        }
    }
}

