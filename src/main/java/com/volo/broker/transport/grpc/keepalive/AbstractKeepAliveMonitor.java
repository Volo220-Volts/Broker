package com.volo.broker.transport.grpc.keepalive;

import org.slf4j.Logger;
import org.springframework.beans.factory.DisposableBean;

import com.volo.broker.transport.grpc.connection.TrackableConnection;
import com.volo.broker.transport.grpc.connection.TrackableConnectionRegistry;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class AbstractKeepAliveMonitor<T extends TrackableConnection> implements DisposableBean {
    private final TrackableConnectionRegistry<T> registry;
    private final ScheduledExecutorService executor;
    private final long timeoutMillis;
    private final Logger logger;

    protected AbstractKeepAliveMonitor(
            TrackableConnectionRegistry<T> registry,
            long timeoutMillis,
            long checkPeriodMillis,
            String threadName,
            Logger logger
    ) {
        this.registry = registry;
        this.timeoutMillis = Math.max(1000, timeoutMillis);
        this.logger = logger;
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            thread.setName(threadName);
            return thread;
        });
        this.executor.scheduleAtFixedRate(
                this::closeStaleConnections,
                Math.max(1000, checkPeriodMillis),
                Math.max(1000, checkPeriodMillis),
                TimeUnit.MILLISECONDS
        );
        logger.info("Started keep alive monitor thread: {}", threadName);
    }

    private void closeStaleConnections() {
        long now = System.currentTimeMillis();

        for (T connection : registry.activeConnections()) {
            if (connection.isClosed()) {
                registry.unregister(connection.connectionId());
                continue;
            }

            if (now - connection.lastSeenAt() > timeoutMillis) {
                logger.debug("Closing stale connection: {}", connection.connectionId());
                registry.unregister(connection.connectionId());
                connection.close();
            }
        }
    }

    @Override
    public void destroy() {
        executor.shutdownNow();
    }
}
