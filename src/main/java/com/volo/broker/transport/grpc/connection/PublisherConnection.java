package com.volo.broker.transport.grpc.connection;

import com.custombroker.grpc.PublisherResponse;
import com.volo.broker.transport.grpc.adapter.SafeGrpcObserver;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class PublisherConnection implements TrackableConnection {
    private final String connectionId = UUID.randomUUID().toString();
    private final SafeGrpcObserver<PublisherResponse> observer;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong lastSeenAt = new AtomicLong(System.currentTimeMillis());

    public PublisherConnection(SafeGrpcObserver<PublisherResponse> observer) {
        this.observer = observer;
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
            observer.onCompleted();
        }
    }
}
