package com.volo.broker.broker.recovery;

import com.volo.broker.broker.message.GlobalLongMessageIdGenerator;
import com.volo.broker.broker.message.MessageIdGenerator;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.queue.settings.*;
import com.volo.broker.persistence.AolMessageStore;
import com.volo.broker.persistence.FileQueueMetadataStore;
import com.volo.broker.persistence.MessageStore;
import com.volo.broker.persistence.QueueMetadataStore;
import com.volo.broker.persistence.aol.api.MessageLogStorage;
import com.volo.broker.persistence.aol.store.AolMessageLogStorage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerRecoveryIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Complete broker recovery without database: queues, metadata, live messages, and ID generator")
    void fullRecovery_restoresQueuesAndLiveMessages_withoutDatabase() throws IOException {
        String ordersQueuePath = "orders.created";
        String paymentsQueuePath = "finance.payments";

        QueueSettings ordersSettings = new QueueSettings(
                PersistenceMode.PERSISTENT,
                PublishAckMode.ENABLED,
                ConsumerAckMode.ENABLED,
                new RedeliveryPolicy(true, 5, true, false),
                new MessageRetentionPolicy(true, 30_000L),
                new QueueLifecyclePolicy(false, false, 0L, true),
                new QueueCapacityPolicy(true, 1000, 5_000_000L, OverflowPolicy.DROP_OLDEST),
                new DispatchPolicy(100, ConsumerSelectionStrategy.ROUND_ROBIN)
        );

        QueueSettings paymentsSettings = new QueueSettings(
                PersistenceMode.PERSISTENT,
                PublishAckMode.ENABLED,
                ConsumerAckMode.ENABLED,
                RedeliveryPolicy.defaults(),
                MessageRetentionPolicy.disabled(),
                QueueLifecyclePolicy.defaults(),
                QueueCapacityPolicy.unlimited(),
                new DispatchPolicy(250, ConsumerSelectionStrategy.LEAST_INFLIGHT)
        );

        // 1. Initial run: declare queues, save messages, ack some messages
        try (MessageLogStorage logStorage = new AolMessageLogStorage(tempDir, false)) {
            QueueMetadataStore metadataStore = new FileQueueMetadataStore(tempDir);
            MessageStore messageStore = new AolMessageStore(logStorage, metadataStore);

            metadataStore.save(ordersQueuePath, ordersSettings);
            metadataStore.save(paymentsQueuePath, paymentsSettings);

            // Persist messages in orders queue
            messageStore.save(ordersQueuePath, message(101L, "order-1"));
            messageStore.save(ordersQueuePath, message(102L, "order-2"));
            messageStore.save(ordersQueuePath, message(103L, "order-3"));

            // Acknowledge order-1
            messageStore.delete(101L);

            // Persist messages in payments queue
            messageStore.save(paymentsQueuePath, message(201L, "pay-1"));

            assertThat(messageStore.findMaxMessageId()).isEqualTo(201L);
        }

        // 2. Simulated restart: completely new memory state, fresh QueueManager, fresh recovery service
        try (MessageLogStorage logStorage = new AolMessageLogStorage(tempDir, false)) {
            QueueMetadataStore metadataStore = new FileQueueMetadataStore(tempDir);
            MessageStore messageStore = new AolMessageStore(logStorage, metadataStore);
            QueueManager queueManager = new InMemoryQueueManager();
            MessageIdGenerator idGenerator = new GlobalLongMessageIdGenerator();

            BrokerRecoveryService recoveryService = new BrokerRecoveryService(
                    metadataStore,
                    messageStore,
                    queueManager,
                    idGenerator
            );

            // Execute recovery
            recoveryService.run(null);

            // Verify queues are restored with exact settings
            Optional<BrokerQueue> recoveredOrders = queueManager.find(ordersQueuePath);
            assertThat(recoveredOrders).isPresent();
            assertThat(recoveredOrders.get().settings()).isEqualTo(ordersSettings);

            Optional<BrokerQueue> recoveredPayments = queueManager.find(paymentsQueuePath);
            assertThat(recoveredPayments).isPresent();
            assertThat(recoveredPayments.get().settings()).isEqualTo(paymentsSettings);

            // Verify order messages (101 was acked, so only 102 and 103 should be recovered)
            BrokerQueue ordersQueue = recoveredOrders.get();
            Optional<QueuedMessage> msg1 = ordersQueue.poll();
            assertThat(msg1).isPresent();
            assertThat(msg1.get().getMessageId()).isEqualTo(102L);
            assertThat(msg1.get().isRedelivered()).isTrue();

            Optional<QueuedMessage> msg2 = ordersQueue.poll();
            assertThat(msg2).isPresent();
            assertThat(msg2.get().getMessageId()).isEqualTo(103L);
            assertThat(msg2.get().isRedelivered()).isTrue();

            assertThat(ordersQueue.poll()).isEmpty();

            // Verify payment messages
            BrokerQueue paymentsQueue = recoveredPayments.get();
            Optional<QueuedMessage> payMsg = paymentsQueue.poll();
            assertThat(payMsg).isPresent();
            assertThat(payMsg.get().getMessageId()).isEqualTo(201L);
            assertThat(payMsg.get().isRedelivered()).isTrue();
            assertThat(paymentsQueue.poll()).isEmpty();

            // Verify message ID generator continues after max ID
            assertThat(idGenerator.nextId()).isEqualTo(202L);
        }
    }

    private static QueuedMessage message(long id, String payload) {
        return new QueuedMessage(
                id,
                payload.getBytes(),
                Map.of(),
                "test.route",
                System.currentTimeMillis(),
                false,
                0
        );
    }
}
