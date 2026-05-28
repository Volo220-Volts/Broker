package com.volo.broker.transport.grpc.adapter;

import io.grpc.stub.StreamObserver;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class SafeGrpcObserver<T> {
    private final StreamObserver<T> delegate;
    private final Object lock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public SafeGrpcObserver(StreamObserver<T> delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    }

    public boolean isClosed() {
        return closed.get();
    }

    public void onNext(T value) {
        if (closed.get()) {
            return;
        }

        synchronized (lock) {
            if (!closed.get()) {
                delegate.onNext(value);
            }
        }
    }

    public void onCompleted() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        synchronized (lock) {
            delegate.onCompleted();
        }
    }

    public void onError(Throwable throwable) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        synchronized (lock) {
            delegate.onError(throwable);
        }
    }
}
