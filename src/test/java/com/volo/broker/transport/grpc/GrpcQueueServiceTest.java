package com.volo.broker.transport.grpc;

import com.custombroker.grpc.DeclareQueueRequest;
import com.custombroker.grpc.DeleteQueueRequest;
import com.custombroker.grpc.QueueOperationResponse;
import com.volo.broker.broker.command.CommandBus;
import com.volo.broker.broker.command.model.DeclareQueueCommand;
import com.volo.broker.broker.command.model.DeleteQueueCommand;
import com.volo.broker.broker.command.model.QueueOperationResult;
import com.volo.broker.transport.grpc.service.GrpcQueueService;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GrpcQueueServiceTest {

    @Test
    void shouldReturnSuccessForDeclareQueue() {
        CommandBus commandBus = mock(CommandBus.class);
        when(commandBus.execute(any(DeclareQueueCommand.class)))
                .thenReturn(QueueOperationResult.ofSuccess());

        GrpcQueueService service = new GrpcQueueService(commandBus);
        @SuppressWarnings("unchecked")
        StreamObserver<QueueOperationResponse> responseObserver = mock(StreamObserver.class);

        service.declareQueue(DeclareQueueRequest.newBuilder().setQueuePath("something.else.queue-name").build(), responseObserver);

        verify(responseObserver).onNext(any(QueueOperationResponse.class));
        verify(responseObserver).onCompleted();
    }

    @Test
    void shouldReturnSuccessForDeleteQueue() {
        CommandBus commandBus = mock(CommandBus.class);
        when(commandBus.execute(any(DeleteQueueCommand.class)))
                .thenReturn(QueueOperationResult.ofSuccess());

        GrpcQueueService service = new GrpcQueueService(commandBus);
        @SuppressWarnings("unchecked")
        StreamObserver<QueueOperationResponse> responseObserver = mock(StreamObserver.class);

        service.deleteQueue(DeleteQueueRequest.newBuilder().setQueuePath("something.else.queue-name").build(), responseObserver);

        verify(responseObserver).onNext(any(QueueOperationResponse.class));
        verify(responseObserver).onCompleted();
    }
}
