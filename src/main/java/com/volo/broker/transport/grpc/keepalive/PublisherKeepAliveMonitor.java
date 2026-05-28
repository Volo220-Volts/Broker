package com.volo.broker.transport.grpc.keepalive;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.volo.broker.transport.grpc.connection.PublisherConnection;
import com.volo.broker.transport.grpc.connection.PublisherConnectionRegistry;

@Component
public class PublisherKeepAliveMonitor extends AbstractKeepAliveMonitor<PublisherConnection> {
    public PublisherKeepAliveMonitor(
            PublisherConnectionRegistry registry,
            @Value("${broker.publisher.keep-alive.timeout-ms:30000}") long timeoutMillis,
            @Value("${broker.publisher.keep-alive.check-period-ms:5000}") long checkPeriodMillis
    ) {
        super(
                registry,
                timeoutMillis,
                checkPeriodMillis,
                "publisher-keep-alive-monitor",
                LoggerFactory.getLogger(PublisherKeepAliveMonitor.class)
        );
    }
}
