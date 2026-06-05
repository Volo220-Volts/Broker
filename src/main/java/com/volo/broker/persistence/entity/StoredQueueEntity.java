package com.volo.broker.persistence.entity;

import com.volo.broker.broker.queue.settings.*;
import jakarta.persistence.*;

@Entity
@Table(name = "broker_queues")
public class StoredQueueEntity {

    @Id
    @Column(name = "queue_path", nullable = false, length = 255, updatable = false)
    private String queuePath;

    @Enumerated(EnumType.STRING)
    @Column(name = "persistence_mode", nullable = false, length = 32)
    private PersistenceMode persistenceMode;

    @Enumerated(EnumType.STRING)
    @Column(name = "publish_ack_mode", nullable = false, length = 32)
    private PublishAckMode publishAckMode;

    @Enumerated(EnumType.STRING)
    @Column(name = "consumer_ack_mode", nullable = false, length = 32)
    private ConsumerAckMode consumerAckMode;

    @Column(name = "redelivery_enabled", nullable = false)
    private boolean redeliveryEnabled;

    @Column(name = "max_delivery_attempts", nullable = false)
    private int maxDeliveryAttempts;

    @Column(name = "requeue_on_disconnect", nullable = false)
    private boolean requeueOnDisconnect;

    @Column(name = "requeue_on_send_failure", nullable = false)
    private boolean requeueOnSendFailure;

    @Column(name = "ttl_enabled", nullable = false)
    private boolean ttlEnabled;

    @Column(name = "ttl_ms", nullable = false)
    private long ttlMs;

    @Column(name = "auto_delete", nullable = false)
    private boolean autoDelete;

    @Column(name = "idle_timeout_enabled", nullable = false)
    private boolean idleTimeoutEnabled;

    @Column(name = "idle_timeout_ms", nullable = false)
    private long idleTimeoutMs;

    @Column(name = "capacity_limited", nullable = false)
    private boolean capacityLimited;

    @Column(name = "max_messages", nullable = false)
    private int maxMessages;

    @Column(name = "max_bytes", nullable = false)
    private long maxBytes;

    @Enumerated(EnumType.STRING)
    @Column(name = "overflow_policy", nullable = false, length = 32)
    private OverflowPolicy overflowPolicy;

    @Column(name = "prefetch_count", nullable = false)
    private int prefetchCount;

    @Enumerated(EnumType.STRING)
    @Column(name = "consumer_selection_strategy", nullable = false, length = 32)
    private ConsumerSelectionStrategy consumerSelectionStrategy;

    @Column(name = "created_at", nullable = false)
    private long createdAt;

    protected StoredQueueEntity() {
    }

    public static StoredQueueEntity from(String queuePath, QueueSettings settings) {
        StoredQueueEntity entity = new StoredQueueEntity();

        entity.queuePath = queuePath;

        entity.persistenceMode = settings.persistenceMode();
        entity.publishAckMode = settings.publishAckMode();
        entity.consumerAckMode = settings.consumerAckMode();

        entity.redeliveryEnabled = settings.redeliveryPolicy().enabled();
        entity.maxDeliveryAttempts = settings.redeliveryPolicy().maxAttempts();
        entity.requeueOnDisconnect = settings.redeliveryPolicy().requeueOnDisconnect();
        entity.requeueOnSendFailure = settings.redeliveryPolicy().requeueOnSendFailure();

        entity.ttlEnabled = settings.retentionPolicy().ttlEnabled();
        entity.ttlMs = settings.retentionPolicy().ttlMs();

        entity.autoDelete = settings.lifecyclePolicy().autoDelete();
        entity.idleTimeoutEnabled = settings.lifecyclePolicy().idleTimeoutEnabled();
        entity.idleTimeoutMs = settings.lifecyclePolicy().idleTimeoutMs();

        entity.capacityLimited = settings.capacityPolicy().limited();
        entity.maxMessages = settings.capacityPolicy().maxMessages();
        entity.maxBytes = settings.capacityPolicy().maxBytes();
        entity.overflowPolicy = settings.capacityPolicy().overflowPolicy();

        entity.prefetchCount = settings.dispatchPolicy().prefetchCount();
        entity.consumerSelectionStrategy = settings.dispatchPolicy().consumerSelectionStrategy();

        entity.createdAt = System.currentTimeMillis();

        return entity;
    }

    public QueueSettings toQueueSettings() {
        return new QueueSettings(
                persistenceMode,
                publishAckMode,
                consumerAckMode,
                new RedeliveryPolicy(
                        redeliveryEnabled,
                        maxDeliveryAttempts,
                        requeueOnDisconnect,
                        requeueOnSendFailure
                ),
                new MessageRetentionPolicy(
                        ttlEnabled,
                        ttlMs
                ),
                new QueueLifecyclePolicy(
                        autoDelete,
                        idleTimeoutEnabled,
                        idleTimeoutMs,
                        true
                ),
                new QueueCapacityPolicy(
                        capacityLimited,
                        maxMessages,
                        maxBytes,
                        overflowPolicy
                ),
                new DispatchPolicy(
                        prefetchCount,
                        consumerSelectionStrategy
                )
        );
    }

    public String getQueuePath() {
        return queuePath;
    }

    public long getCreatedAt() {
        return createdAt;
    }
}