package com.volo.broker.transport.grpc;

import com.custombroker.grpc.BrokerResponse;
import com.custombroker.grpc.MessageDelivery;
import com.custombroker.grpc.Message;
import com.volo.broker.engine.ConsumerChannel;
import com.volo.broker.engine.QueuedMessage;
import com.volo.broker.transport.model.SafeGrpcObserver;

import io.grpc.stub.StreamObserver;
import java.util.UUID;

public class GrpcConsumerChannelAdapter implements ConsumerChannel {

    private final String consumerTag = UUID.randomUUID().toString();
    private final SafeGrpcObserver<BrokerResponse> observer;

    public GrpcConsumerChannelAdapter(StreamObserver<BrokerResponse> responseObserver) {
        this.observer = new SafeGrpcObserver<>(responseObserver);
    }

    @Override
    public String consumerTag() {
        return consumerTag;
    }

    @Override
    public boolean canAcceptMore() {
        return !observer.isClosed();
    }

    @Override
    public void send(com.volo.broker.engine.MessageDelivery delivery) {
        if (observer.isClosed()) return;

        QueuedMessage qm = delivery.message();

        Message protoMsg = GrpcMessageConverters.toProtoMessage(qm);

        MessageDelivery protoDelivery = MessageDelivery.newBuilder()
                .setDeliveryTag(delivery.deliveryTag())
                .setMessage(protoMsg)
                .build();

        BrokerResponse response = BrokerResponse.newBuilder()
                .setDelivery(protoDelivery)
                .build();

        observer.onNext(response);
    }

    @Override
    public void close(String reason) {
        observer.onCompleted();
    }

    @Override
    public void onAck(long deliveryTag) {
    }

    @Override
    public void onNack(long deliveryTag, boolean requeue) {
    }
}
