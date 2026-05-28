package com.volo.broker.transport.grpc;

import com.custombroker.grpc.PublisherRequest;
import com.custombroker.grpc.PublisherResponse;
import com.custombroker.grpc.PublisherServiceGrpc;
import com.custombroker.grpc.PublishAction;
import com.custombroker.grpc.PublishNack;
import com.volo.broker.core.command.CommandBus;
import com.volo.broker.core.command.model.PublishCommand;
import com.volo.broker.core.command.model.PublishResult;
import com.volo.broker.core.error.BrokerErrorCode;
import com.volo.broker.core.error.BrokerException;
import com.volo.broker.transport.grpc.adapter.SafeGrpcObserver;
import com.volo.broker.transport.grpc.connection.PublisherConnection;
import com.volo.broker.transport.grpc.connection.PublisherConnectionRegistry;
import com.volo.broker.transport.grpc.mapper.GrpcErrorMapper;
import com.volo.broker.transport.grpc.mapper.GrpcMessageConverters;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class GrpcPublisherService extends PublisherServiceGrpc.PublisherServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(GrpcPublisherService.class);

    private final CommandBus commandBus;
    private final PublisherConnectionRegistry registry;

    public GrpcPublisherService(CommandBus commandBus, PublisherConnectionRegistry registry) {
        this.commandBus = Objects.requireNonNull(commandBus, "commandBus must not be null");
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
    }

    @Override
    public StreamObserver<PublisherRequest> openPublisherChannel(StreamObserver<PublisherResponse> responseObserver) {
        PublisherConnection connection = new PublisherConnection(new SafeGrpcObserver<>(responseObserver));
        registry.register(connection);

        return new StreamObserver<>() {
            @Override
            public void onNext(PublisherRequest request) {
                if (connection.isClosed()) {
                    return;
                }

                connection.markSeen();

                try {
                    handleRequest(connection, request);
                } catch (BrokerException exception) {
                    connection.send(PublisherResponse.newBuilder()
                            .setNack(PublishNack.newBuilder()
                                    .setClientPublishId(safeClientPublishId(getClientPublishId(request)))
                                    .setError(GrpcErrorMapper.toProto(exception))
                                    .build())
                            .build());
                } catch (Throwable throwable) {
                    log.error("Unexpected publisher request error", throwable);
                    connection.send(PublisherResponse.newBuilder()
                            .setNack(PublishNack.newBuilder()
                                    .setClientPublishId(safeClientPublishId(getClientPublishId(request)))
                                    .setError(GrpcErrorMapper.internalError(throwable))
                                    .build())
                            .build());
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

    private void handleRequest(PublisherConnection connection, PublisherRequest request) {
        if (request == null || request.getRequestCase() == PublisherRequest.RequestCase.REQUEST_NOT_SET) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "Publisher request must contain one request type");
        }

        switch (request.getRequestCase()) {
            case PUBLISH -> handlePublish(connection, request.getPublish());
            case PING -> connection.send(GrpcMessageConverters.toPongResponse());
            case CLOSE -> closeConnection(connection);
            case REQUEST_NOT_SET -> throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "Publisher request must contain one request type");
        }
    }

    private void handlePublish(PublisherConnection connection, PublishAction action) {
        String clientPublishId = action.getClientPublishId();
        try {
            PublishCommand command = GrpcMessageConverters.toPublishCommand(action);
            PublishResult result = commandBus.execute(command);
            connection.send(GrpcMessageConverters.toPublisherResponse(clientPublishId, result));
        } catch (BrokerException exception) {
            connection.send(GrpcMessageConverters.toPublisherResponse(
                    clientPublishId,
                    PublishResult.nack(exception.code(), exception.getMessage())
            ));
        } catch (Throwable throwable) {
            log.error("Publish failed", throwable);
            connection.send(GrpcMessageConverters.toPublisherResponse(
                    clientPublishId,
                    PublishResult.nack(BrokerErrorCode.INTERNAL_ERROR, "Message was not accepted by broker")
            ));
        }
    }

    private void closeConnection(PublisherConnection connection) {
        registry.unregister(connection.connectionId());
        connection.close();
    }

    private static String getClientPublishId(PublisherRequest request) {
        if (request == null || request.getRequestCase() != PublisherRequest.RequestCase.PUBLISH) {
            return "";
        }
        return request.getPublish().getClientPublishId();
    }

    private static String safeClientPublishId(String clientPublishId) {
        return clientPublishId == null ? "" : clientPublishId;
    }
}
