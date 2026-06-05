package com.volo.broker;

import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;

public class TestStreamObserver<T> implements StreamObserver<T> {
    private final List<T> values = new ArrayList<>();
    private Throwable error;
    private boolean completed;

    @Override
    public void onNext(T value) {
        values.add(value);
    }

    @Override
    public void onError(Throwable throwable) {
        this.error = throwable;
    }

    @Override
    public void onCompleted() {
        this.completed = true;
    }

    public List<T> values() {
        return values;
    }

    public T singleValue() {
        if (values.size() != 1) {
            throw new AssertionError("Expected exactly one value, but got: " + values.size());
        }

        return values.get(0);
    }

    public Throwable error() {
        return error;
    }

    public boolean completed() {
        return completed;
    }
}