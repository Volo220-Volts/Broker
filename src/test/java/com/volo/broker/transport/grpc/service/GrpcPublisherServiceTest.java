package com.volo.broker.transport.grpc.service;

import com.custombroker.grpc.*;
import com.google.protobuf.ByteString;
import com.volo.broker.TestStreamObserver;
import com.volo.broker.broker.command.CommandBus;
import com.volo.broker.broker.command.model.PublishCommand;
import com.volo.broker.broker.command.model.PublishResult;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.transport.grpc.connection.PublisherConnectionRegistry;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class GrpcPublisherServiceTest {

    private final CommandBus commandBus = mock(CommandBus.class);
    private final PublisherConnectionRegistry connectionRegistry = new PublisherConnectionRegistry();

    private final GrpcPublisherService service = new GrpcPublisherService(
            commandBus,
            connectionRegistry
    );

    @Test
    void openPublisherChannel_shouldSendAckWhenPublishAcceptedAndResponseRequired() {
        when(commandBus.execute(any(PublishCommand.class)))
                .thenReturn(PublishResult.ack(1L, 1));

        TestStreamObserver<PublisherResponse> responseObserver = new TestStreamObserver<>();

        StreamObserver<PublisherRequest> requestObserver =
                service.openPublisherChannel(responseObserver);

        requestObserver.onNext(PublisherRequest.newBuilder()
                .setPublish(PublishAction.newBuilder()
                        .setClientPublishId(1L)
                        .setRoutingKey("orders.created")
                        .setPayload(ByteString.copyFrom("payload", StandardCharsets.UTF_8))
                        .build())
                .build());

        assertThat(responseObserver.values()).hasSize(1);

        PublisherResponse response = responseObserver.singleValue();

        assertThat(response.hasAck()).isTrue();
        assertThat(response.getAck().getClientPublishId()).isEqualTo(1L);
        assertThat(response.getAck().getRoutedQueueCount()).isEqualTo(1);
    }

    // @Test
    // void openPublisherChannel_shouldNotSendResponseWhenAckDisabledAndPublishAccepted() {
    //     when(commandBus.execute(any(PublishCommand.class)))
    //             .thenReturn(PublishResult.ack(100L, 1));

    //     TestStreamObserver<PublisherResponse> responseObserver = new TestStreamObserver<>();

    //     StreamObserver<PublisherRequest> requestObserver =
    //             service.openPublisherChannel(responseObserver);

    //     requestObserver.onNext(PublisherRequest.newBuilder()
    //             .setPublish(PublishAction.newBuilder()
    //                     .setClientPublishId("pub-1")
    //                     .setRoutingKey("metrics.runtime")
    //                     .setPayload(ByteString.copyFrom("payload", StandardCharsets.UTF_8))
    //                     .build())
    //             .build());

    //     assertThat(responseObserver.values()).isEmpty();
    // }

    @Test
    void openPublisherChannel_shouldSendNackWhenPublishRejected() {
        when(commandBus.execute(any(PublishCommand.class)))
                .thenReturn(PublishResult.nack(
                        1L,
                        BrokerErrorCode.QUEUE_NOT_FOUND,
                        "No route"
                ));

        TestStreamObserver<PublisherResponse> responseObserver = new TestStreamObserver<>();

        StreamObserver<PublisherRequest> requestObserver =
                service.openPublisherChannel(responseObserver);

        requestObserver.onNext(PublisherRequest.newBuilder()
                .setPublish(PublishAction.newBuilder()
                        .setClientPublishId(1L)
                        .setRoutingKey("missing.route")
                        .setPayload(ByteString.copyFrom("payload", StandardCharsets.UTF_8))
                        .build())
                .build());

        PublisherResponse response = responseObserver.singleValue();

        assertThat(response.hasNack()).isTrue();
        assertThat(response.getNack().getClientPublishId()).isEqualTo(1L);
        assertThat(response.getNack().getError().getCode()).isEqualTo(ErrorCode.QUEUE_NOT_FOUND);
    }

    @Test
    void openPublisherChannel_shouldRespondWithPongOnPing() {
        TestStreamObserver<PublisherResponse> responseObserver = new TestStreamObserver<>();

        StreamObserver<PublisherRequest> requestObserver =
                service.openPublisherChannel(responseObserver);

        requestObserver.onNext(PublisherRequest.newBuilder()
                .setPing(KeepAlive.newBuilder()
                        .setTimestamp(123L)
                        .build())
                .build());

        PublisherResponse response = responseObserver.singleValue();

        assertThat(response.hasPong()).isTrue();
        assertThat(response.getPong().getTimestamp()).isGreaterThanOrEqualTo(123L);
    }

    @Test
    void openPublisherChannel_shouldCompleteOnCloseAction() {
        TestStreamObserver<PublisherResponse> responseObserver = new TestStreamObserver<>();

        StreamObserver<PublisherRequest> requestObserver =
                service.openPublisherChannel(responseObserver);

        requestObserver.onNext(PublisherRequest.newBuilder()
                .setClose(ClosePublisherAction.newBuilder()
                        .setReason("client-close")
                        .build())
                .build());

        assertThat(responseObserver.completed()).isTrue();
    }
}