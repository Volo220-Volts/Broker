package com.volo.broker.persistence.entity;

import java.sql.Types;

import org.hibernate.annotations.JdbcTypeCode;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

@Entity
@Table(name = "broker_messages",
    indexes = {
                @Index(name = "idx_broker_messages_queue_path", columnList = "queue_path"),
                @Index(name = "idx_broker_messages_queue_created", columnList = "queue_path, created_at"),
                @Index(name = "idx_broker_messages_created_at", columnList = "created_at")
    }
)
public class StoredMessageEntity {
    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private Long id;

    @Column(name = "queue_path", nullable = false, length = 255)
    private String queuePath;

    @Column(name = "routing_key", length = 255)
    private String routingKey;

    @JdbcTypeCode(Types.VARBINARY)
    @Column(name = "payload", nullable = false)
    private byte[] payload;

    @Column(name = "headers_json", length = 8192)
    private String headersJson;

    @Column(name = "created_at", nullable = false)
    private Long createdAt;

    @Column(name = "redelivered", nullable = false)
    private boolean redelivered;

    @Column(name = "delivery_attempt", nullable = false)
    private int deliveryAttempt;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getQueuePath() {
        return queuePath;
    }

    public void setQueuePath(String queuePath) {
        this.queuePath = queuePath;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public byte[] getPayload() {
        return payload == null ? new byte[0] : payload.clone();
    }

    public void setPayload(byte[] payload) {
        this.payload = payload == null ? new byte[0] : payload.clone();
    }

    public String getHeadersJson() {
        return headersJson;
    }

    public void setHeadersJson(String headersJson) {
        this.headersJson = headersJson;
    }


    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }

    public boolean isRedelivered() {
        return redelivered;
    }

    public void setRedelivered(boolean redelivered){
        this.redelivered = redelivered;
    }

    public int getDeliveryAttempt() {
        return deliveryAttempt;
    }

    public void setDeliveryAttempt(int attempts){
        deliveryAttempt = attempts;
    }

}
