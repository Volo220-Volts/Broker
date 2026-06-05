package com.volo.broker.persistence;

import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.persistence.dto.StoredQueueDto;
import com.volo.broker.persistence.entity.StoredQueueEntity;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;

@Component
public class JpaQueueStore implements QueueStore {
    private final StoredQueueRepository repository;

    public JpaQueueStore(StoredQueueRepository repository) {
        this.repository = Objects.requireNonNull(repository);
    }

    @Override
    @Transactional
    public void save(BrokerQueue queue) {
        repository.save(
                StoredQueueEntity.from(
                        queue.queuePath(),
                        queue.settings()
                )
        );
    }

    @Override
    @Transactional
    public void delete(String queuePath) {
        repository.deleteById(queuePath);
    }

    @Override
    @Transactional(readOnly = true)
    public List<StoredQueueDto> loadAll() {
        return repository.findAll()
                .stream()
                .map(entity -> new StoredQueueDto(
                        entity.getQueuePath(),
                        entity.toQueueSettings()
                ))
                .toList();
    }
}