package com.volo.broker.broker.queue;

import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.exception.QueueOverflowException;
import com.volo.broker.broker.queue.settings.OverflowPolicy;
import com.volo.broker.broker.queue.settings.QueueCapacityPolicy;
import com.volo.broker.broker.queue.settings.QueueSettings;
import com.volo.broker.broker.router.QueuePath;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class BrokerQueue {
    private final QueuePath path;
    private final QueueSettings settings;
    private final Deque<QueuedMessage> messages = new ArrayDeque<>();
    private final long createdAt;

    private long lastActivityAt;
    private long totalBytes;

    private BrokerQueue(QueuePath path, QueueSettings settings) {
        this.path = Objects.requireNonNull(path, "path must not be null");
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
        this.createdAt = System.currentTimeMillis();
        this.lastActivityAt = createdAt;
    }

    public static BrokerQueue create(QueuePath path, QueueSettings settings) {
        return new BrokerQueue(path, settings);
    }

    public synchronized EnqueueResult enqueue(QueuedMessage message) {
        Objects.requireNonNull(message, "message must not be null");

        List<QueuedMessage> dropped = applyOverflowPolicy(message);

        messages.addLast(message);
        totalBytes += message.sizeBytes();
        touch();

        return dropped.isEmpty()
                ? EnqueueResult.ofAccepted()
                : EnqueueResult.ofAcceptedWithDropped(dropped);
    }

    public synchronized Optional<QueuedMessage> poll() {
        QueuedMessage message = messages.pollFirst();

        if (message == null) {
            return Optional.empty();
        }

        totalBytes -= message.sizeBytes();
        touch();

        return Optional.of(message);
    }

    public synchronized Optional<QueuedMessage> pollOldest() {
        return poll();
    }

    public synchronized void requeue(QueuedMessage message) {
        Objects.requireNonNull(message, "message must not be null");

        ensureCapacityBeforeEnqueue(message);

        messages.addFirst(message);
        totalBytes += message.sizeBytes();
        touch();
    }

    public synchronized int size() {
        return messages.size();
    }

    public synchronized long sizeBytes() {
        return totalBytes;
    }

    public synchronized boolean isEmpty() {
        return messages.isEmpty();
    }

    public QueuePath path() {
        return path;
    }

    public String name() {
        return path.value();
    }

    public QueueSettings settings() {
        return settings;
    }

    public long createdAt() {
        return createdAt;
    }

    public synchronized long lastActivityAt() {
        return lastActivityAt;
    }

    public synchronized void touch() {
        lastActivityAt = System.currentTimeMillis();
    }

    private void ensureCapacityBeforeEnqueue(QueuedMessage message) {
        if (!settings.capacityPolicy().limited()) {
            return;
        }

        boolean messageLimitExceeded = settings.capacityPolicy().maxMessages() > 0
                && messages.size() >= settings.capacityPolicy().maxMessages();

        boolean bytesLimitExceeded = settings.capacityPolicy().maxBytes() > 0
                && totalBytes + message.sizeBytes() > settings.capacityPolicy().maxBytes();

        if (!messageLimitExceeded && !bytesLimitExceeded) {
            return;
        }

        OverflowPolicy overflowPolicy = settings.capacityPolicy().overflowPolicy();

        if (overflowPolicy == OverflowPolicy.DROP_OLDEST) {
            QueuedMessage dropped = messages.pollFirst();
            if (dropped != null) {
                totalBytes -= dropped.sizeBytes();
            }
            return;
        }

        if (overflowPolicy == OverflowPolicy.DROP_NEWEST) {
            throw new QueueOverflowException("Incoming message was dropped because queue is full: " + name());
        }

        throw new QueueOverflowException("Queue capacity exceeded: " + name());
    }

    private List<QueuedMessage> applyOverflowPolicy(QueuedMessage incomingMessage) {
        if (!settings.capacityPolicy().limited()) {
            return List.of();
        }

        if (!capacityExceededAfterAdding(incomingMessage)) {
            return List.of();
        }

        return switch (settings.capacityPolicy().overflowPolicy()) {
            case REJECT_PUBLISH -> throw new QueueOverflowException(
                    "Queue capacity exceeded: " + name()
            );

            case DROP_NEWEST -> throw new QueueOverflowException(
                    "Incoming message was dropped because queue is full: " + name()
            );

            case DROP_OLDEST -> dropOldestUntilFits(incomingMessage);
        };
    }

    private boolean capacityExceededAfterAdding(QueuedMessage incomingMessage) {
        QueueCapacityPolicy policy = settings.capacityPolicy();

        if (!policy.limited()) {
            return false;
        }

        boolean maxMessagesExceeded = policy.maxMessages() > 0
                && messages.size() + 1 > policy.maxMessages();

        boolean maxBytesExceeded = policy.maxBytes() > 0
                && totalBytes + incomingMessage.sizeBytes() > policy.maxBytes();

        return maxMessagesExceeded || maxBytesExceeded;
    }

    private List<QueuedMessage> dropOldestUntilFits(QueuedMessage incomingMessage) {
        List<QueuedMessage> droppedMessages = new ArrayList<>();

        while (!messages.isEmpty() && capacityExceededAfterAdding(incomingMessage)) {
            QueuedMessage dropped = messages.pollFirst();

            if (dropped != null) {
                totalBytes -= dropped.sizeBytes();
                droppedMessages.add(dropped);
            }
        }

        if (capacityExceededAfterAdding(incomingMessage)) {
            throw new QueueOverflowException(
                    "Incoming message is too large for queue capacity: " + name()
            );
        }

        return droppedMessages;
    }
}
