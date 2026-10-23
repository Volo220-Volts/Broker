package com.volo.broker.transport.grpc.connection;

import com.custombroker.grpc.PublisherResponse;
import com.volo.broker.transport.grpc.SafeGrpcObserver;
import com.volo.broker.transport.grpc.flow.PublisherFlowGate;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class PublisherConnection implements TrackableConnection {
    private final String connectionId = UUID.randomUUID().toString();
    private final SafeGrpcObserver<PublisherResponse> observer;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong lastSeenAt = new AtomicLong(System.currentTimeMillis());
    private volatile PublisherFlowGate flowGate;

    public PublisherConnection(SafeGrpcObserver<PublisherResponse> observer) {
        this(observer, null);
    }

    public PublisherConnection(SafeGrpcObserver<PublisherResponse> observer, PublisherFlowGate flowGate) {
        this.observer = observer;
        this.flowGate = flowGate;
    }

    public void attachFlowGate(PublisherFlowGate flowGate) {
        this.flowGate = flowGate;
    }

    public PublisherFlowGate flowGate() {
        return flowGate;
    }

    @Override
    public boolean isPaused() {
        return flowGate != null && flowGate.isPaused();
    }

    @Override
    public String connectionId() {
        return connectionId;
    }

    @Override
    public long lastSeenAt() {
        return lastSeenAt.get();
    }

    @Override
    public boolean isClosed() {
        return closed.get() || observer.isClosed();
    }

    @Override
    public void markSeen() {
        lastSeenAt.set(System.currentTimeMillis());
    }

    public void send(PublisherResponse response) {
        if (!isClosed()) {
            observer.onNext(response);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (flowGate != null) {
                flowGate.close();
            }
            observer.onCompleted();
        }
    }
}
