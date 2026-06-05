package com.volo.broker.transport.grpc.connection;

public interface TrackableConnection {
    String connectionId();
    long lastSeenAt();
    boolean isClosed();
    void close();
    void markSeen();
}
