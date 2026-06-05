package com.volo.broker.persistence;

import com.volo.broker.persistence.entity.StoredQueueEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StoredQueueRepository extends JpaRepository<StoredQueueEntity, String> {
}