package com.volo.broker;

import com.volo.broker.broker.message.QueuedMessage;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class TestMessages {
    private TestMessages() {
    }

    public static QueuedMessage message(long id) {
        return new QueuedMessage(
                id,
                ("payload-" + id).getBytes(StandardCharsets.UTF_8),
                Map.of(),
                "orders.created",
                System.currentTimeMillis(),
                false,
                0
        );
    }

    public static QueuedMessage expiredMessage(long id) {
        return new QueuedMessage(
                id,
                ("payload-" + id).getBytes(StandardCharsets.UTF_8),
                Map.of(),
                "orders.created",
                System.currentTimeMillis() - 60_000,
                false,
                0
        );
    }
}
