package com.volo.broker.transport.grpc.connection;

import com.custombroker.grpc.BrokerResponse;
import com.volo.broker.TestStreamObserver;
import com.volo.broker.broker.command.CommandBus;
import com.volo.broker.transport.grpc.SafeGrpcObserver;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class ConsumerConnectionRegistryTest {

    @Test
    void register_shouldStoreConnection() {
        ConsumerConnectionRegistry registry = new ConsumerConnectionRegistry();

        ConsumerConnection connection = new ConsumerConnection(
                new SafeGrpcObserver<>(new TestStreamObserver<BrokerResponse>()),
                mock(CommandBus.class)
        );

        registry.register(connection);

        assertThat(registry.find(connection.connectionId()))
                .contains(connection);
    }

    @Test
    void unregister_shouldRemoveConnection() {
        ConsumerConnectionRegistry registry = new ConsumerConnectionRegistry();

        ConsumerConnection connection = new ConsumerConnection(
                new SafeGrpcObserver<>(new TestStreamObserver<BrokerResponse>()),
                mock(CommandBus.class)
        );

        registry.register(connection);
        registry.unregister(connection.connectionId());

        assertThat(registry.find(connection.connectionId()))
                .isEmpty();
    }

    @Test
    void activeConnections_shouldReturnRegisteredConnections() {
        ConsumerConnectionRegistry registry = new ConsumerConnectionRegistry();

        ConsumerConnection connection = new ConsumerConnection(
                new SafeGrpcObserver<>(new TestStreamObserver<BrokerResponse>()),
                mock(CommandBus.class)
        );

        registry.register(connection);

        assertThat(registry.activeConnections())
                .containsExactly(connection);
    }
}
