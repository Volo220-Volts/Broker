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
import com.volo.broker.broker.flow.FlowState;
import com.volo.broker.broker.flow.FlowTransition;
import com.volo.broker.broker.queue.settings.FlowControlPolicy;

public final class BrokerQueue {
    private final QueuePath path;
    private final QueueSettings settings;
    private final Deque<QueuedMessage> messages = new ArrayDeque<>();
    private final long createdAt;

    private volatile FlowState flowState = FlowState.OPEN;
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

        FlowTransition transition = evaluateTransition();

        return dropped.isEmpty()
                ? EnqueueResult.ofAccepted(transition)
                : EnqueueResult.ofAcceptedWithDropped(dropped, transition);
    }

    public synchronized PollResult poll() {
        QueuedMessage message = messages.pollFirst();

        if (message == null) {
            return PollResult.empty();
        }

        totalBytes -= message.sizeBytes();
        touch();

        FlowTransition transition = evaluateTransition();

        return PollResult.of(message, transition);
    }

    public synchronized boolean canAccept(QueuedMessage message) {
        Objects.requireNonNull(message, "message must not be null");
        if (!settings.capacityPolicy().limited()) {
            return true;
        }
        if (!capacityExceededAfterAdding(message)) {
            return true;
        }
        QueueCapacityPolicy policy = settings.capacityPolicy();
        if (policy.overflowPolicy() == OverflowPolicy.DROP_OLDEST) {
            return policy.maxBytes() <= 0 || message.sizeBytes() <= policy.maxBytes();
        }
        return false;
    }

    public synchronized FlowTransition requeue(QueuedMessage message) {
        Objects.requireNonNull(message, "message must not be null");

        messages.addFirst(message);
        totalBytes += message.sizeBytes();
        touch();

        return evaluateTransition();
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

    public synchronized void touch() {
        lastActivityAt = System.currentTimeMillis();
    }

    //#region getters
    public QueuePath path() {
        return path;
    }

    public String queuePath() {
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

    public FlowState flowState() {
        return flowState;
    }

    //#endregion

    private List<QueuedMessage> applyOverflowPolicy(QueuedMessage incomingMessage) {
        if (!settings.capacityPolicy().limited()) {
            return List.of();
        }

        if (!capacityExceededAfterAdding(incomingMessage)) {
            return List.of();
        }

        return switch (settings.capacityPolicy().overflowPolicy()) {
            case REJECT_PUBLISH -> throw new QueueOverflowException(
                    "Queue capacity exceeded: " + queuePath()
            );

            case DROP_NEWEST -> throw new QueueOverflowException(
                    "Incoming message was dropped because queue is full: " + queuePath()
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
                    "Incoming message is too large for queue capacity: " + queuePath()
            );
        }

        return droppedMessages;
    }

    private FlowTransition evaluateTransition() {
        FlowControlPolicy policy = settings.flowControlPolicy();
        if (policy == null || !policy.enabled()) {
            return FlowTransition.NONE;
        }

        if (flowState == FlowState.OPEN) {
            if (isHighWatermarkExceeded(policy)) {
                this.flowState = FlowState.BLOCKED;
                return FlowTransition.TO_BLOCKED;
            }
        } else if (flowState == FlowState.BLOCKED) {
            if (isLowWatermarkCleared(policy)) {
                this.flowState = FlowState.OPEN;
                return FlowTransition.TO_OPEN;
            }
        }
        return FlowTransition.NONE;
    }

    private boolean isHighWatermarkExceeded(FlowControlPolicy policy) {
        boolean msgExceeded = policy.highWatermarkMessages() > 0 && messages.size() >= policy.highWatermarkMessages();
        boolean bytesExceeded = policy.highWatermarkBytes() > 0 && totalBytes >= policy.highWatermarkBytes();
        return msgExceeded || bytesExceeded;
    }

    private boolean isLowWatermarkCleared(FlowControlPolicy policy) {
        boolean msgCleared = policy.highWatermarkMessages() <= 0 || messages.size() <= policy.lowWatermarkMessages();
        boolean bytesCleared = policy.highWatermarkBytes() <= 0 || totalBytes <= policy.lowWatermarkBytes();
        return msgCleared && bytesCleared;
    }
}
