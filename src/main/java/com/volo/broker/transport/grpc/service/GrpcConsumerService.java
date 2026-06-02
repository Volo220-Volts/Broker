package com.volo.broker.transport.grpc.service;

import com.custombroker.grpc.BrokerResponse;
import com.custombroker.grpc.ConsumerRequest;
import com.custombroker.grpc.ConsumerServiceGrpc;
import com.custombroker.grpc.KeepAlive;
import com.volo.broker.broker.command.CommandBus;
import com.volo.broker.broker.command.model.AckCommand;
import com.volo.broker.broker.command.model.DisconnectCommand;
import com.volo.broker.broker.command.model.NackCommand;
import com.volo.broker.broker.command.model.SubscribeCommand;
import com.volo.broker.transport.grpc.SafeGrpcObserver;
import com.volo.broker.transport.grpc.adapter.GrpcConsumerChannelAdapter;
import com.volo.broker.transport.grpc.connection.ConsumerConnection;
import com.volo.broker.transport.grpc.connection.ConsumerConnectionRegistry;
import com.volo.broker.transport.grpc.mapper.GrpcErrorMapper;

import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class GrpcConsumerService extends ConsumerServiceGrpc.ConsumerServiceImplBase {
    private final CommandBus commandBus;
    private final ConsumerConnectionRegistry registry;

    public GrpcConsumerService(CommandBus commandBus, ConsumerConnectionRegistry registry) {
        this.commandBus = Objects.requireNonNull(commandBus, "commandBus must not be null");
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
    }

    @Override
    public StreamObserver<ConsumerRequest> openChannel(StreamObserver<BrokerResponse> responseObserver) {
        ConsumerConnection connection = new ConsumerConnection(new SafeGrpcObserver<>(responseObserver), commandBus);
        GrpcConsumerChannelAdapter adapter = new GrpcConsumerChannelAdapter(connection);
        registry.register(connection);

        return new StreamObserver<>() {
            @Override
            public void onNext(ConsumerRequest request) {
                if (connection.isClosed()) {
                    return;
                }

                connection.markSeen();

                try {
                    handleRequest(connection, adapter, request);
                } catch (Throwable throwable) {
                    connection.send(BrokerResponse.newBuilder()
                            .setError(GrpcErrorMapper.internalError(throwable))
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

    private void handleRequest(
            ConsumerConnection connection,
            GrpcConsumerChannelAdapter adapter,
            ConsumerRequest request
    ) {
        switch (request.getRequestCase()) {
            case SUBSCRIBE -> commandBus.execute(new SubscribeCommand(
                    request.getSubscribe().getQueueName(),
                    request.getSubscribe().getPrefetchCount(),
                    adapter.consumerTag(),
                    adapter
            ));
            case ACK -> commandBus.execute(new AckCommand(
                    adapter.consumerTag(),
                    request.getAck().getDeliveryTag(),
                    request.getAck().getMessageId()
            ));
            case NACK -> commandBus.execute(new NackCommand(
                    adapter.consumerTag(),
                    request.getNack().getDeliveryTag(),
                    request.getNack().getMessageId(),
                    request.getNack().getRequeue()
            ));
            case PING -> connection.send(BrokerResponse.newBuilder()
                    .setPong(KeepAlive.newBuilder()
                            .setTimestamp(System.currentTimeMillis())
                            .build())
                    .build());
            case REQUEST_NOT_SET -> connection.send(BrokerResponse.newBuilder()
                    .setError(GrpcErrorMapper.fromCode(
                            com.volo.broker.broker.error.BrokerErrorCode.INVALID_REQUEST,
                            "Consumer request must contain one request type"
                    ))
                    .build());
        }
    }

    private void closeConnection(ConsumerConnection connection) {
        registry.unregister(connection.connectionId());
        commandBus.execute(new DisconnectCommand(connection.consumerTag()));
        connection.close();
    }
}
