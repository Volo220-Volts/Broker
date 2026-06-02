package com.volo.broker.core.validation;

import org.junit.jupiter.api.Test;

import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.validation.QueuePathValidator;

import static org.junit.jupiter.api.Assertions.*;

class QueuePathValidatorTest {

    @Test
    void shouldValidateCleanQueuePath() {
        QueuePathValidator validator = new QueuePathValidator();

        QueuePath queuePath = validator.validateAndParse("something.else.queue-name");

        assertEquals("something.else.queue-name", queuePath.value());
        assertEquals("queue-name", queuePath.queueName());
    }

    @Test
    void shouldRejectQueuePathWithInvalidCharacters() {
        QueuePathValidator validator = new QueuePathValidator();

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateAndParse("something/else.queue")
        );

        assertEquals("queue path may contain only letters, digits, dots, underscores and hyphens", exception.getMessage());
    }
}
