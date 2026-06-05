package com.volo.broker.transport.grpc.connection;

import com.custombroker.grpc.PublisherResponse;
import com.volo.broker.TestStreamObserver;
import com.volo.broker.transport.grpc.SafeGrpcObserver;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PublisherConnectionTest {

    @Test
    void markSeen_shouldUpdateLastSeenAt() throws InterruptedException {
        PublisherConnection connection = new PublisherConnection(
                new SafeGrpcObserver<>(new TestStreamObserver<PublisherResponse>())
        );

        long before = connection.lastSeenAt();

        Thread.sleep(5);

        connection.markSeen();

        assertThat(connection.lastSeenAt())
                .isGreaterThan(before);
    }

    @Test
    void close_shouldMarkConnectionAsClosed() {
        PublisherConnection connection = new PublisherConnection(
                new SafeGrpcObserver<>(new TestStreamObserver<PublisherResponse>())
        );

        connection.close();

        assertThat(connection.isClosed()).isTrue();
    }
}