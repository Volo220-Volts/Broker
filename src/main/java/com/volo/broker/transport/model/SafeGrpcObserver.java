package com.volo.broker.transport.model;

import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.stub.StreamObserver;

public final class SafeGrpcObserver<T> {
    private final StreamObserver<T> delegate;
    private final Object lock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public SafeGrpcObserver(StreamObserver<T> delegate) {
        this.delegate = delegate;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public void onNext(T value) {
        if (closed.get()) return;

        synchronized (lock) {
            if (closed.get()) return;
            try {
                delegate.onNext(value);
            } catch (Throwable ex) {
                closed.set(true);
                throw ex;
            }
        }
    }

    public void onCompleted() {
        if (!closed.compareAndSet(false, true)) return;

        synchronized (lock) {
            try {
                delegate.onCompleted();
            } catch (Throwable ignored) {
            }
        }
    }

    public void onError(Throwable t) {
        if (!closed.compareAndSet(false, true)) return;

        synchronized (lock) {
            try {
                delegate.onError(t);
            } catch (Throwable ignored) {
            }
        }
    }
}
