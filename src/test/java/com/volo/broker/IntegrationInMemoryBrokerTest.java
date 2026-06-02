package com.volo.broker;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.volo.broker.broker.command.handler.DisconnectCommandHandler;
import com.volo.broker.broker.command.model.DisconnectCommand;
import com.volo.broker.broker.dispatch.DispatchManager;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.message.DeliveryGuarantee;
import com.volo.broker.broker.message.MessageDelivery;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.InMemoryDequeManager;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.subscription.ConsumerChannel;
import com.volo.broker.broker.subscription.DefaultSubscriptionManager;
import com.volo.broker.broker.subscription.SubscriptionManager;
import com.volo.broker.persistence.MessageStore;

@ExtendWith(MockitoExtension.class)
class IntegrationInMemoryBrokerTest {

    @Mock
    MessageStore messageStore;

    @Test
    void crashReconnect_shouldRedeliverUnackedMessageBeforeNextMessage() throws Exception {
        String queueName = "orders";

        QueueManager queueManager = new InMemoryDequeManager();
        SubscriptionManager subscriptionManager = new DefaultSubscriptionManager();
        DispatchManager dispatchManager = new DispatchManager(queueManager, subscriptionManager);
        QueueDispatchScheduler scheduler = new QueueDispatchScheduler(dispatchManager, queueManager, 2);

        DisconnectCommandHandler disconnectHandler = new DisconnectCommandHandler(
                subscriptionManager,
                queueManager,
                scheduler
        );

        try {
            queueManager.createQueue(queueName);
            queueManager.enqueue(queueName, message(1L, queueName));
            queueManager.enqueue(queueName, message(2L, queueName));
            queueManager.enqueue(queueName, message(3L, queueName));

            TestConsumerChannel firstConsumer = new TestConsumerChannel();
            subscriptionManager.register(queueName, firstConsumer, 1);

            scheduler.signal(queueName);
            assertTrue(awaitDeliveries(firstConsumer, 1));

            MessageDelivery firstDelivery = firstConsumer.deliveries().get(0);
            assertEquals(1L, firstDelivery.message().getMessageId());

            disconnectHandler.handle(new DisconnectCommand(firstConsumer.consumerTag()));

            TestConsumerChannel secondConsumer = new TestConsumerChannel();
            subscriptionManager.register(queueName, secondConsumer, 1);

            scheduler.signal(queueName);
            assertTrue(awaitDeliveries(secondConsumer, 1));

            MessageDelivery redelivered = secondConsumer.deliveries().get(0);
            assertEquals(1L, redelivered.message().getMessageId());
        } finally {
            scheduler.shutdown();
        }
    }

    private boolean awaitDeliveries(TestConsumerChannel channel, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            if (channel.deliveries().size() >= expected) {
                return true;
            }
            Thread.sleep(10);
        }
        return false;
    }

    private QueuedMessage message(long id, String queueName) {
        return new QueuedMessage(
                id,
                ("payload-" + id).getBytes(StandardCharsets.UTF_8),
                Map.of(),
                queueName,
                System.currentTimeMillis(),
                DeliveryGuarantee.AT_LEAST_ONCE,
                false,
                0
        );
    }

    static final class TestConsumerChannel implements ConsumerChannel {
        private final String consumerTag = UUID.randomUUID().toString();
        private final List<MessageDelivery> deliveries = new CopyOnWriteArrayList<>();
        private volatile boolean open = true;

        @Override
        public String consumerTag() {
            return consumerTag;
        }

        @Override
        public boolean canAcceptMore() {
            return open;
        }

        @Override
        public void send(MessageDelivery delivery) {
            deliveries.add(delivery);
        }

        @Override
        public void close(String reason) {
            open = false;
        }

        @Override
        public void onAck(long deliveryTag) {
        }

        @Override
        public void onNack(long deliveryTag, boolean requeue) {
        }

        public List<MessageDelivery> deliveries() {
            return deliveries;
        }

    }
}
