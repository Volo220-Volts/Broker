package com.volo.broker.broker.router;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class QueuePathTest {

    @Test
    void of_shouldParseValidQueuePath() {
        QueuePath path = QueuePath.of("orders.created");

        assertThat(path.value()).isEqualTo("orders.created");
        assertThat(path.segments()).containsExactly("orders", "created");
    }

    @Test
    void of_shouldTrimValue() {
        QueuePath path = QueuePath.of("  orders.created  ");

        assertThat(path.value()).isEqualTo("orders.created");
    }

    @Test
    void of_shouldRejectBlankPath() {
        assertThatThrownBy(() -> QueuePath.of(" "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void of_shouldRejectWildcardStar() {
        assertThatThrownBy(() -> QueuePath.of("orders.*"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void of_shouldRejectWildcardHash() {
        assertThatThrownBy(() -> QueuePath.of("orders.#"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void of_shouldRejectEmptySegments() {
        assertThatThrownBy(() -> QueuePath.of("orders..created"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}