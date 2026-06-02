package com.volo.broker.transport.grpc.service;

import com.custombroker.grpc.DeclareQueueRequest;
import com.custombroker.grpc.DeleteQueueRequest;
import com.custombroker.grpc.QueueOperationResponse;
import com.custombroker.grpc.QueueServiceGrpc;
import com.volo.broker.broker.command.CommandBus;
import com.volo.broker.broker.command.model.DeclareQueueCommand;
import com.volo.broker.broker.command.model.DeleteQueueCommand;
import com.volo.broker.broker.command.model.QueueOperationResult;
import com.volo.broker.transport.grpc.mapper.GrpcErrorMapper;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class GrpcQueueService extends QueueServiceGrpc.QueueServiceImplBase {

    private final CommandBus commandBus;

    public GrpcQueueService(CommandBus commandBus) {
        this.commandBus = Objects.requireNonNull(commandBus, "commandBus must not be null");
    }

    @Override
    public void declareQueue(DeclareQueueRequest request, StreamObserver<QueueOperationResponse> responseObserver) {
        QueueOperationResult result = commandBus.execute(new DeclareQueueCommand(request.getQueuePath()));
        sendResponse(result, responseObserver);
    }

    @Override
    public void deleteQueue(DeleteQueueRequest request, StreamObserver<QueueOperationResponse> responseObserver) {
        QueueOperationResult result = commandBus.execute(new DeleteQueueCommand(request.getQueuePath()));
        sendResponse(result, responseObserver);
    }

    private void sendResponse(QueueOperationResult result, StreamObserver<QueueOperationResponse> responseObserver) {
        QueueOperationResponse.Builder builder = QueueOperationResponse.newBuilder()
                .setSuccess(result.success());

        if (!result.success()) {
            builder.setError(GrpcErrorMapper.fromCode(result.errorCode(), result.errorMessage()));
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }
}
