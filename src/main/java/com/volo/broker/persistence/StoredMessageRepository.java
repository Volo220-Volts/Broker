package com.volo.broker.persistence;

import com.volo.broker.persistence.entity.StoredMessageEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface StoredMessageRepository extends JpaRepository<StoredMessageEntity, Long> {

    List<StoredMessageEntity> findAllByOrderByIdAsc();

    @Query("select coalesce(max(m.id), 0) from StoredMessageEntity m")
    long findMaxMessageId();

    @Modifying
    @Query("delete from StoredMessageEntity m where m.queuePath = :queuePath")
    int deleteByQueuePath(String queuePath);

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("delete from StoredMessageEntity m where m.id = :messageId")
    int deleteByMessageId(@Param("messageId") Long messageId);
}
