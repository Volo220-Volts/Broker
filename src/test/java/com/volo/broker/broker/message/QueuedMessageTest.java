package com.volo.broker.broker.message;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class QueuedMessageTest {

    @Test
    void asRedelivered_shouldMarkMessageAsRedeliveredAndIncrementAttempt() {
        QueuedMessage message = new QueuedMessage(
                1L,
                "payload".getBytes(),
                Map.of("type", "test"),
                "orders.created",
                1000L,
                false,
                0
        );

        QueuedMessage redelivered = message.asRedelivered();

        assertThat(redelivered.getMessageId()).isEqualTo(1L);
        assertThat(redelivered.isRedelivered()).isTrue();
        assertThat(redelivered.getDeliveryAttempt()).isEqualTo(1);
        assertThat(redelivered.getPayload()).isEqualTo(message.getPayload());
        assertThat(redelivered.getHeaders()).isEqualTo(message.getHeaders());
    }

    @Test
    void sizeBytes_shouldIncludePayloadAndHeaders() {
        QueuedMessage message = new QueuedMessage(
                1L,
                "payload".getBytes(),
                Map.of("h1", "v1", "h2", "v2"),
                "orders.created",
                1000L,
                false,
                0
        );

        assertThat(message.sizeBytes()).isGreaterThanOrEqualTo("payload".getBytes().length);
    }

    @Test
    void constructor_shouldProtectPayloadFromExternalMutation() {
        byte[] payload = "payload".getBytes();

        QueuedMessage message = new QueuedMessage(
                1L,
                payload,
                Map.of(),
                "orders.created",
                1000L,
                false,
                0
        );

        payload[0] = 'X';

        assertThat(new String(message.getPayload())).isEqualTo("payload");
    }
}