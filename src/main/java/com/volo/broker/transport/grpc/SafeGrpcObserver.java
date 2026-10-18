package com.volo.broker.transport.grpc;

import io.grpc.stub.StreamObserver;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public final class SafeGrpcObserver<T> {
    private final StreamObserver<T> delegate;
    private final ReentrantLock lock = new ReentrantLock();
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

        lock.lock();
        try {
            if (!closed.get()) {
                delegate.onNext(value);
            }
        } finally {
            lock.unlock();
        }
    }

    public void onCompleted() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        lock.lock();
        try {
            delegate.onCompleted();
        } finally {
            lock.unlock();
        }
    }

    public void onError(Throwable throwable) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        lock.lock();
        try {
            delegate.onError(throwable);
        } finally {
            lock.unlock();
        }
    }
}
