package com.volo.broker.transport.grpc.service;

import com.custombroker.grpc.PublisherRequest;
import com.custombroker.grpc.PublisherResponse;
import com.custombroker.grpc.PublisherServiceGrpc;
import com.custombroker.grpc.PublishAction;
import com.custombroker.grpc.PublishNack;
import com.volo.broker.broker.command.CommandBus;
import com.volo.broker.broker.command.model.PublishCommand;
import com.volo.broker.broker.command.model.PublishResult;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.error.BrokerException;
import com.volo.broker.broker.flow.QueueFlowController;
import com.volo.broker.transport.grpc.SafeGrpcObserver;
import com.volo.broker.transport.grpc.connection.PublisherConnection;
import com.volo.broker.transport.grpc.connection.PublisherConnectionRegistry;
import com.volo.broker.transport.grpc.flow.PublisherFlowGate;
import com.volo.broker.transport.grpc.mapper.GrpcErrorMapper;
import com.volo.broker.transport.grpc.mapper.GrpcMessageConverters;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

@Component
public class GrpcPublisherService extends PublisherServiceGrpc.PublisherServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(GrpcPublisherService.class);

    private final CommandBus commandBus;
    private final PublisherConnectionRegistry registry;
    private final QueueFlowController flowController;

    public GrpcPublisherService(CommandBus commandBus, PublisherConnectionRegistry registry) {
        this(commandBus, registry, null);
    }

    @Autowired
    public GrpcPublisherService(
            CommandBus commandBus,
            PublisherConnectionRegistry registry,
            @Nullable QueueFlowController flowController
    ) {
        this.commandBus = Objects.requireNonNull(commandBus, "commandBus must not be null");
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        this.flowController = flowController;
    }

    @Override
    public StreamObserver<PublisherRequest> openPublisherChannel(StreamObserver<PublisherResponse> responseObserver) {
        ServerCallStreamObserver<PublisherResponse> serverCall = responseObserver instanceof ServerCallStreamObserver<PublisherResponse> sco
                ? sco
                : null;

        if (serverCall != null) {
            serverCall.disableAutoRequest();
        }

        PublisherConnection connection = new PublisherConnection(new SafeGrpcObserver<>(responseObserver));
        PublisherFlowGate gate = new PublisherFlowGate(serverCall, flowController, connection);
        connection.attachFlowGate(gate);
        registry.register(connection);

        if (serverCall != null) {
            serverCall.request(1);
        }

        return new StreamObserver<>() {
            @Override
            public void onNext(PublisherRequest request) {
                if (connection.isClosed()) {
                    return;
                }

                connection.markSeen();

                List<String> routedQueues = List.of();
                try {
                    routedQueues = handleRequest(connection, request);
                } catch (BrokerException exception) {
                    connection.send(PublisherResponse.newBuilder()
                            .setNack(PublishNack.newBuilder()
                                    .setClientPublishId(getClientPublishId(request))
                                    .setError(GrpcErrorMapper.toProto(exception))
                                    .build())
                            .build());
                } catch (Throwable throwable) {
                    log.error("Unexpected publisher request error", throwable);
                    connection.send(PublisherResponse.newBuilder()
                            .setNack(PublishNack.newBuilder()
                                    .setClientPublishId(getClientPublishId(request))
                                    .setError(GrpcErrorMapper.internalError(throwable))
                                    .build())
                            .build());
                } finally {
                    gate.afterRequestProcessed(routedQueues);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                closeConnection(connection);
            }

            @Override
            public void onCompleted() {
                closeConnection(connection);
            }
        };
    }

    private List<String> handleRequest(PublisherConnection connection, PublisherRequest request) {
        if (request == null || request.getRequestCase() == PublisherRequest.RequestCase.REQUEST_NOT_SET) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "Publisher request must contain one request type");
        }

        switch (request.getRequestCase()) {
            case PUBLISH -> {
                return handlePublish(connection, request.getPublish());
            }
            case PING -> {
                connection.send(GrpcMessageConverters.toPongResponse());
                return List.of();
            }
            case CLOSE -> {
                closeConnection(connection);
                return List.of();
            }
            case REQUEST_NOT_SET -> throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "Publisher request must contain one request type");
        }
        return List.of();
    }

    private List<String> handlePublish(PublisherConnection connection, PublishAction action) {
        Long clientPublishId = action.getClientPublishId();

        if (connection.flowGate() != null && connection.flowGate().isOverdue()) {
            connection.flowGate().clearOverdue();
            if (flowController != null) {
                flowController.recordOverdueRejection();
            }
            PublishResult result = PublishResult.nack(
                    clientPublishId,
                    BrokerErrorCode.RESOURCE_EXHAUSTED,
                    "Flow control timeout exceeded: downstream queue is blocked"
            );
            connection.send(GrpcMessageConverters.toPublisherResponse(result));
            return List.of();
        }

        try {
            PublishCommand command = GrpcMessageConverters.toPublishCommand(action);
            PublishResult result = commandBus.execute(command);
            connection.send(GrpcMessageConverters.toPublisherResponse(result));
            return result.routedQueuePaths();
        } catch (BrokerException exception) {
            PublishResult result = PublishResult.nack(clientPublishId, exception.code(), exception.getMessage());
            connection.send(GrpcMessageConverters.toPublisherResponse(result));
            return List.of();
        } catch (Throwable throwable) {
            log.error("Publish failed", throwable);
            PublishResult result = PublishResult.nack(clientPublishId, BrokerErrorCode.INTERNAL_ERROR, "Message was not accepted by broker");
            connection.send(GrpcMessageConverters.toPublisherResponse(result));
            return List.of();
        }
    }

    private void closeConnection(PublisherConnection connection) {
        registry.unregister(connection.connectionId());
        connection.close();
    }

    private static Long getClientPublishId(PublisherRequest request) {
        if (request == null || request.getRequestCase() != PublisherRequest.RequestCase.PUBLISH) {
            return 0L;
        }
        return request.getPublish().getClientPublishId();
    }
}
