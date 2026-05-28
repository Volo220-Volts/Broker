package com.volo.broker.transport.grpc.connection;

import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class ConsumerConnectionRegistry implements TrackableConnectionRegistry<ConsumerConnection> {
    private final ConcurrentMap<String, ConsumerConnection> connections = new ConcurrentHashMap<>();

    @Override
    public void register(ConsumerConnection connection) {
        connections.put(connection.connectionId(), connection);
    }

    @Override
    public Optional<ConsumerConnection> find(String connectionId) {
        return Optional.ofNullable(connections.get(connectionId));
    }

    @Override
    public void unregister(String connectionId) {
        connections.remove(connectionId);
    }

    @Override
    public Collection<ConsumerConnection> activeConnections() {
        return connections.values();
    }
}
