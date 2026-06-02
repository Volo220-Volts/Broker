package com.volo.broker.domain.router;

import org.junit.jupiter.api.Test;

import com.volo.broker.broker.router.QueuePath;

import static org.junit.jupiter.api.Assertions.*;

class QueuePathTest {

    @Test
    void shouldParseSingleSegmentQueuePath() {
        QueuePath queuePath = QueuePath.of("orders");

        assertEquals("orders", queuePath.value());
        assertEquals("", queuePath.routePath());
        assertEquals("orders", queuePath.queueName());
        assertEquals(1, queuePath.segments().size());
    }

    @Test
    void shouldParseNestedQueuePath() {
        QueuePath queuePath = QueuePath.of("something.else.queue-name");

        assertEquals("something.else.queue-name", queuePath.value());
        assertEquals("something.else", queuePath.routePath());
        assertEquals("queue-name", queuePath.queueName());
        assertEquals(3, queuePath.segments().size());
        assertTrue(queuePath.hasRoutePath());
    }

    @Test
    void shouldRejectQueuePathWithWildcards() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> QueuePath.of("something.*.queue")
        );

        assertEquals("queue path must not contain wildcards", exception.getMessage());
    }

    @Test
    void shouldRejectBlankQueuePath() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> QueuePath.of("   ")
        );

        assertEquals("queue path is required", exception.getMessage());
    }
}
