package com.volo.broker.broker.queue;

import com.volo.broker.TestMessages;
import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.queue.exception.QueueOverflowException;
import com.volo.broker.broker.queue.settings.OverflowPolicy;
import com.volo.broker.broker.router.QueuePath;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class BrokerQueueTest {

    @Test
    void poll_shouldReturnMessagesInFifoOrder() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        queue.enqueue(TestMessages.message(1));
        queue.enqueue(TestMessages.message(2));

        assertThat(queue.poll()).hasValueSatisfying(message ->
                assertThat(message.getMessageId()).isEqualTo(1)
        );

        assertThat(queue.poll()).hasValueSatisfying(message ->
                assertThat(message.getMessageId()).isEqualTo(2)
        );
    }

    @Test
    void requeue_shouldReturnMessageToHead() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        queue.enqueue(TestMessages.message(1));
        queue.enqueue(TestMessages.message(2));

        var first = queue.poll().orElseThrow();
        queue.requeue(first.asRedelivered());

        assertThat(queue.poll()).hasValueSatisfying(message -> {
            assertThat(message.getMessageId()).isEqualTo(1);
            assertThat(message.isRedelivered()).isTrue();
        });
    }

    @Test
    void enqueue_shouldRejectPublish_whenQueueIsFull() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("metrics.runtime"),
                TestQueueSettings.withCapacity(1, OverflowPolicy.REJECT_PUBLISH)
        );

        queue.enqueue(TestMessages.message(1));

        assertThatThrownBy(() -> queue.enqueue(TestMessages.message(2)))
                .isInstanceOf(QueueOverflowException.class);
    }

    @Test
    void enqueue_shouldDropOldest_whenOverflowPolicyDropOldest() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("metrics.runtime"),
                TestQueueSettings.withCapacity(1, OverflowPolicy.DROP_OLDEST)
        );

        EnqueueResult firstResult = queue.enqueue(TestMessages.message(1));
        EnqueueResult secondResult = queue.enqueue(TestMessages.message(2));

        assertThat(firstResult.accepted()).isTrue();
        assertThat(secondResult.accepted()).isTrue();
        assertThat(secondResult.droppedMessages())
                .extracting("messageId")
                .containsExactly(1L);

        assertThat(queue.poll()).hasValueSatisfying(message ->
                assertThat(message.getMessageId()).isEqualTo(2)
        );
    }

    @Test
    void enqueue_shouldRejectNewest_whenOverflowPolicyDropNewest() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("metrics.runtime"),
                TestQueueSettings.withCapacity(1, OverflowPolicy.DROP_NEWEST)
        );

        queue.enqueue(TestMessages.message(1));

        assertThatThrownBy(() -> queue.enqueue(TestMessages.message(2)))
                .isInstanceOf(QueueOverflowException.class);

        assertThat(queue.poll()).hasValueSatisfying(message ->
                assertThat(message.getMessageId()).isEqualTo(1)
        );
    }
}