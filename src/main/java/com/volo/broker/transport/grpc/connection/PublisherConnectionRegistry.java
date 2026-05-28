package com.volo.broker.transport.grpc.connection;

import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class PublisherConnectionRegistry implements TrackableConnectionRegistry<PublisherConnection> {
    private final ConcurrentMap<String, PublisherConnection> connections = new ConcurrentHashMap<>();

    @Override
    public void register(PublisherConnection connection) {
        connections.put(connection.connectionId(), connection);
    }

    @Override
    public Optional<PublisherConnection> find(String connectionId) {
        return Optional.ofNullable(connections.get(connectionId));
    }

    @Override
    public void unregister(String connectionId) {
        connections.remove(connectionId);
    }

    @Override
    public Collection<PublisherConnection> activeConnections() {
        return connections.values();
    }
}
