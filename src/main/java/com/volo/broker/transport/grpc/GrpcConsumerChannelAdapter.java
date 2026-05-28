package com.volo.broker.transport.grpc;

import com.custombroker.grpc.BrokerResponse;
import com.custombroker.grpc.MessageDelivery;
import com.volo.broker.domain.subscription.ConsumerChannel;
import com.volo.broker.transport.grpc.adapter.SafeGrpcObserver;
import com.volo.broker.transport.grpc.connection.ConsumerConnection;
import com.volo.broker.transport.grpc.mapper.GrpcMessageConverters;

import io.grpc.stub.StreamObserver;

public class GrpcConsumerChannelAdapter implements ConsumerChannel {
    private final ConsumerConnection connection;

    public GrpcConsumerChannelAdapter(ConsumerConnection connection) {
        this.connection = connection;
    }

    public GrpcConsumerChannelAdapter(StreamObserver<BrokerResponse> responseObserver) {
        this(new ConsumerConnection(new SafeGrpcObserver<>(responseObserver), null));
    }

    @Override
    public String consumerTag() {
        return connection.consumerTag();
    }

    @Override
    public boolean canAcceptMore() {
        return !connection.isClosed();
    }

    @Override
    public void send(com.volo.broker.domain.message.MessageDelivery delivery) {
        MessageDelivery protoDelivery = MessageDelivery.newBuilder()
                .setDeliveryTag(delivery.deliveryTag())
                .setMessage(GrpcMessageConverters.toProtoMessage(delivery.message()))
                .setRedelivered(delivery.message().isRedelivered())
                .setDeliveryAttempt(delivery.message().getDeliveryAttempt())
                .build();

        try {
            connection.send(BrokerResponse.newBuilder()
                    .setDelivery(protoDelivery)
                    .build());
        } catch (Exception e) {
            connection.close();
        }
    }

    @Override
    public void close(String reason) {
        connection.close();
    }

    @Override
    public void onAck(long deliveryTag) {
    }

    @Override
    public void onNack(long deliveryTag, boolean requeue) {
    }
}
