package com.volo.broker.transport.grpc.keepalive;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.volo.broker.transport.grpc.connection.ConsumerConnection;
import com.volo.broker.transport.grpc.connection.ConsumerConnectionRegistry;

@Component
public class ConsumerKeepAliveMonitor extends AbstractKeepAliveMonitor<ConsumerConnection> {
    public ConsumerKeepAliveMonitor(
            ConsumerConnectionRegistry registry,
            @Value("${broker.consumer.keep-alive.timeout-ms:30000}") long timeoutMillis,
            @Value("${broker.consumer.keep-alive.check-period-ms:5000}") long checkPeriodMillis
    ) {
        super(
                registry,
                timeoutMillis,
                checkPeriodMillis,
                "consumer-keep-alive-monitor",
                LoggerFactory.getLogger(ConsumerKeepAliveMonitor.class)
        );
    }
}
