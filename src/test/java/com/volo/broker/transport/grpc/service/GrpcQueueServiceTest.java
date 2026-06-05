package com.volo.broker.transport.grpc.service;

import com.custombroker.grpc.*;
import com.volo.broker.TestStreamObserver;
import com.volo.broker.broker.command.CommandBus;
import com.volo.broker.broker.command.model.DeclareQueueCommand;
import com.volo.broker.broker.command.model.DeleteQueueCommand;
import com.volo.broker.broker.command.model.QueueOperationResult;
import com.volo.broker.broker.error.BrokerErrorCode;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class GrpcQueueServiceTest {

    private final CommandBus commandBus = mock(CommandBus.class);
    private final GrpcQueueService service = new GrpcQueueService(commandBus);

    @Test
    void declareQueue_shouldReturnSuccessResponse() {
        when(commandBus.execute(any(DeclareQueueCommand.class)))
                .thenReturn(QueueOperationResult.ofSuccess());

        TestStreamObserver<QueueOperationResponse> observer = new TestStreamObserver<>();

        service.declareQueue(
                DeclareQueueRequest.newBuilder()
                        .setQueuePath("orders.created")
                        .setSettings(defaultProtoSettings())
                        .build(),
                observer
        );

        QueueOperationResponse response = observer.singleValue();

        assertThat(response.getSuccess()).isTrue();
        assertThat(observer.completed()).isTrue();

        verify(commandBus).execute(any(DeclareQueueCommand.class));
    }

    @Test
    void declareQueue_shouldReturnErrorResponse() {
        when(commandBus.execute(any(DeclareQueueCommand.class)))
                .thenReturn(QueueOperationResult.ofFailure(
                        BrokerErrorCode.QUEUE_ALREADY_EXISTS,
                        "Queue already exists"
                ));

        TestStreamObserver<QueueOperationResponse> observer = new TestStreamObserver<>();

        service.declareQueue(
                DeclareQueueRequest.newBuilder()
                        .setQueuePath("orders.created")
                        .setSettings(defaultProtoSettings())
                        .build(),
                observer
        );

        QueueOperationResponse response = observer.singleValue();

        assertThat(response.getSuccess()).isFalse();
        assertThat(response.getError().getCode()).isEqualTo(ErrorCode.QUEUE_ALREADY_EXISTS);
        assertThat(response.getError().getMessage()).isEqualTo("Queue already exists");
        assertThat(observer.completed()).isTrue();
    }

    @Test
    void deleteQueue_shouldReturnSuccessResponse() {
        when(commandBus.execute(any(DeleteQueueCommand.class)))
                .thenReturn(QueueOperationResult.ofSuccess());

        TestStreamObserver<QueueOperationResponse> observer = new TestStreamObserver<>();

        service.deleteQueue(
                DeleteQueueRequest.newBuilder()
                        .setQueuePath("orders.created")
                        .build(),
                observer
        );

        QueueOperationResponse response = observer.singleValue();

        assertThat(response.getSuccess()).isTrue();
        assertThat(observer.completed()).isTrue();

        verify(commandBus).execute(any(DeleteQueueCommand.class));
    }

    @Test
    void deleteQueue_shouldReturnQueueNotFound() {
        when(commandBus.execute(any(DeleteQueueCommand.class)))
                .thenReturn(QueueOperationResult.ofFailure(
                        BrokerErrorCode.QUEUE_NOT_FOUND,
                        "Queue not found"
                ));

        TestStreamObserver<QueueOperationResponse> observer = new TestStreamObserver<>();

        service.deleteQueue(
                DeleteQueueRequest.newBuilder()
                        .setQueuePath("orders.created")
                        .build(),
                observer
        );

        QueueOperationResponse response = observer.singleValue();

        assertThat(response.getSuccess()).isFalse();
        assertThat(response.getError().getCode()).isEqualTo(ErrorCode.QUEUE_NOT_FOUND);
    }

    private QueueSettings defaultProtoSettings() {
        return QueueSettings.newBuilder()
                .setPersistenceMode(PersistenceMode.PERSISTENT)
                .setPublishAckMode(PublishAckMode.PUBLISH_ACK_ENABLED)
                .setConsumerAckMode(ConsumerAckMode.CONSUMER_ACK_ENABLED)
                .setRedeliveryPolicy(RedeliveryPolicy.newBuilder()
                        .setEnabled(true)
                        .setMaxAttempts(5)
                        .setRequeueOnDisconnect(true)
                        .setRequeueOnSendFailure(true)
                        .build())
                .setRetentionPolicy(MessageRetentionPolicy.newBuilder()
                        .setTtlEnabled(false)
                        .setTtlMs(0)
                        .build())
                .setLifecyclePolicy(QueueLifecyclePolicy.newBuilder()
                        .setAutoDelete(false)
                        .setIdleTimeoutEnabled(false)
                        .setIdleTimeoutMs(0)
                        .build())
                .setCapacityPolicy(QueueCapacityPolicy.newBuilder()
                        .setLimited(false)
                        .setMaxMessages(0)
                        .setMaxBytes(0)
                        .setOverflowPolicy(OverflowPolicy.REJECT_PUBLISH)
                        .build())
                .setDispatchPolicy(DispatchPolicy.newBuilder()
                        .setPrefetchCount(10)
                        .setConsumerSelectionStrategy(ConsumerSelectionStrategy.ROUND_ROBIN)
                        .build())
                .build();
    }
}