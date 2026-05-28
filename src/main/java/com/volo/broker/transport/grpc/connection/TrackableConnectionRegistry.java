package com.volo.broker.transport.grpc.connection;

import java.util.Collection;
import java.util.Optional;

public interface TrackableConnectionRegistry<T extends TrackableConnection> {
    void register(T connection);
    Optional<T> find(String connectionId);
    void unregister(String connectionId);
    Collection<T> activeConnections();
}
