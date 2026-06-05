package com.volo.broker.transport.grpc.service;

import com.custombroker.grpc.*;
import com.volo.broker.TestStreamObserver;
import com.volo.broker.broker.command.CommandBus;
import com.volo.broker.broker.command.model.AckCommand;
import com.volo.broker.broker.command.model.NackCommand;
import com.volo.broker.broker.command.model.SubscribeCommand;
import com.volo.broker.transport.grpc.connection.ConsumerConnectionRegistry;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class GrpcConsumerServiceTest {

    private final CommandBus commandBus = mock(CommandBus.class);
    private final ConsumerConnectionRegistry connectionRegistry = new ConsumerConnectionRegistry();

    private final GrpcConsumerService service = new GrpcConsumerService(
            commandBus,
            connectionRegistry
    );

    @Test
    void openChannel_shouldRouteSubscribeCommand() {
        TestStreamObserver<BrokerResponse> responseObserver = new TestStreamObserver<>();

        StreamObserver<ConsumerRequest> requestObserver =
                service.openChannel(responseObserver);

        requestObserver.onNext(ConsumerRequest.newBuilder()
                .setSubscribe(SubscribeAction.newBuilder()
                        .setQueueName("orders.created")
                        .setPrefetchCount(10)
                        .build())
                .build());

        verify(commandBus).execute(any(SubscribeCommand.class));
    }

    @Test
    void openChannel_shouldRouteAckCommand() {
        TestStreamObserver<BrokerResponse> responseObserver = new TestStreamObserver<>();

        StreamObserver<ConsumerRequest> requestObserver =
                service.openChannel(responseObserver);

        requestObserver.onNext(ConsumerRequest.newBuilder()
                .setSubscribe(SubscribeAction.newBuilder()
                        .setQueueName("orders.created")
                        .setPrefetchCount(10)
                        .build())
                .build());

        requestObserver.onNext(ConsumerRequest.newBuilder()
                .setAck(AckAction.newBuilder()
                        .setDeliveryTag(1L)
                        .setMessageId(100L)
                        .build())
                .build());

        verify(commandBus).execute(any(SubscribeCommand.class));
        verify(commandBus).execute(any(AckCommand.class));
    }

    @Test
    void openChannel_shouldRouteNackCommand() {
        TestStreamObserver<BrokerResponse> responseObserver = new TestStreamObserver<>();

        StreamObserver<ConsumerRequest> requestObserver =
                service.openChannel(responseObserver);

        requestObserver.onNext(ConsumerRequest.newBuilder()
                .setSubscribe(SubscribeAction.newBuilder()
                        .setQueueName("orders.created")
                        .setPrefetchCount(10)
                        .build())
                .build());

        requestObserver.onNext(ConsumerRequest.newBuilder()
                .setNack(NackAction.newBuilder()
                        .setDeliveryTag(1L)
                        .setMessageId(100L)
                        .setRequeue(true)
                        .build())
                .build());

        verify(commandBus).execute(any(SubscribeCommand.class));
        verify(commandBus).execute(any(NackCommand.class));
    }

    @Test
    void openChannel_shouldRespondWithPongOnPing() {
        TestStreamObserver<BrokerResponse> responseObserver = new TestStreamObserver<>();

        StreamObserver<ConsumerRequest> requestObserver =
                service.openChannel(responseObserver);

        requestObserver.onNext(ConsumerRequest.newBuilder()
                .setPing(KeepAlive.newBuilder()
                        .setTimestamp(123L)
                        .build())
                .build());

        BrokerResponse response = responseObserver.singleValue();

        assertThat(response.hasPong()).isTrue();
        assertThat(response.getPong().getTimestamp()).isGreaterThanOrEqualTo(123L);
    }

    @Test
    void openChannel_shouldCompleteOnClientCompleted() {
        TestStreamObserver<BrokerResponse> responseObserver = new TestStreamObserver<>();

        StreamObserver<ConsumerRequest> requestObserver =
                service.openChannel(responseObserver);

        requestObserver.onCompleted();

        assertThat(responseObserver.completed()).isTrue();
    }
}