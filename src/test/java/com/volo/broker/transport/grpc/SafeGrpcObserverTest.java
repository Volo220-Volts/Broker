package com.volo.broker.transport.grpc;

import com.volo.broker.TestStreamObserver;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SafeGrpcObserverTest {

    @Test
    void onNext_shouldForwardValueWhenOpen() {
        TestStreamObserver<String> delegate = new TestStreamObserver<>();
        SafeGrpcObserver<String> observer = new SafeGrpcObserver<>(delegate);

        observer.onNext("hello");

        assertThat(delegate.values()).containsExactly("hello");
    }

    @Test
    void onCompleted_shouldCompleteOnlyOnce() {
        TestStreamObserver<String> delegate = new TestStreamObserver<>();
        SafeGrpcObserver<String> observer = new SafeGrpcObserver<>(delegate);

        observer.onCompleted();
        observer.onCompleted();

        assertThat(delegate.completed()).isTrue();
    }

    @Test
    void onNext_shouldBeIgnoredAfterCompleted() {
        TestStreamObserver<String> delegate = new TestStreamObserver<>();
        SafeGrpcObserver<String> observer = new SafeGrpcObserver<>(delegate);

        observer.onCompleted();
        observer.onNext("ignored");

        assertThat(delegate.values()).isEmpty();
    }

    @Test
    void onError_shouldCloseObserver() {
        TestStreamObserver<String> delegate = new TestStreamObserver<>();
        SafeGrpcObserver<String> observer = new SafeGrpcObserver<>(delegate);

        RuntimeException exception = new RuntimeException("boom");

        observer.onError(exception);
        observer.onNext("ignored");

        assertThat(delegate.error()).isEqualTo(exception);
        assertThat(delegate.values()).isEmpty();
    }
}