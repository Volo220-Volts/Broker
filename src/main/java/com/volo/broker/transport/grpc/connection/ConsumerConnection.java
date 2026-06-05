package com.volo.broker.transport.grpc.connection;

import com.custombroker.grpc.BrokerResponse;
import com.volo.broker.broker.command.CommandBus;
import com.volo.broker.broker.command.model.DisconnectCommand;
import com.volo.broker.transport.grpc.SafeGrpcObserver;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class ConsumerConnection implements TrackableConnection {
    private final String connectionId = UUID.randomUUID().toString();
    private final String consumerTag = UUID.randomUUID().toString();
    private final SafeGrpcObserver<BrokerResponse> observer;
    private final CommandBus commandBus;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong lastSeenAt = new AtomicLong(System.currentTimeMillis());

    public ConsumerConnection(SafeGrpcObserver<BrokerResponse> observer, CommandBus commandBus) {
        this.observer = Objects.requireNonNull(observer, "observer must not be null");
        this.commandBus = commandBus;
    }

    @Override
    public String connectionId() {
        return connectionId;
    }

    public String consumerTag() {
        return consumerTag;
    }

    @Override
    public long lastSeenAt() {
        return lastSeenAt.get();
    }

    @Override
    public boolean isClosed() {
        return closed.get() || observer.isClosed();
    }

    @Override
    public void markSeen() {
        lastSeenAt.set(System.currentTimeMillis());
    }

    public void send(BrokerResponse response) {
        if (!isClosed()) {
            observer.onNext(response);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                if (commandBus != null) {
                    commandBus.execute(new DisconnectCommand(consumerTag));
                }
            } finally {
                observer.onCompleted();
            }
        }
    }
}
