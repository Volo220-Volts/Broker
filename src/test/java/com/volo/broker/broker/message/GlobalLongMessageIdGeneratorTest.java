package com.volo.broker.broker.message;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import com.volo.broker.persistence.StoredMessageRepository;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalLongMessageIdGeneratorTest {

    @Mock
    private StoredMessageRepository repository;

    @Test
    void nextId_shouldGenerateIncreasingPositiveIds() {
        GlobalLongMessageIdGenerator generator = new GlobalLongMessageIdGenerator(repository);

        long first = generator.nextId();
        long second = generator.nextId();
        long third = generator.nextId();

        assertThat(first).isPositive();
        assertThat(second).isGreaterThan(first);
        assertThat(third).isGreaterThan(second);
    }
}