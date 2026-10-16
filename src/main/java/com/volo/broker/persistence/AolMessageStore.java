package com.volo.broker.persistence;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.persistence.aol.api.MessageLogStorage;
import com.volo.broker.persistence.aol.api.WriteResult;
import com.volo.broker.persistence.dto.StoredMessageDto;

/**
 * Serves as an adapter between the broker persistence layer and the append-only log system for message storage.
 */
public class AolMessageStore implements MessageStore, Closeable {

    private final MessageLogStorage messageLogStorage;
    private final QueueMetadataStore queueMetadataStore;
    private final Map<Long, RouteKey> idToRouteKey = new ConcurrentHashMap<>();
    private final AtomicLong maxMessageId = new AtomicLong(0L);

    public AolMessageStore(MessageLogStorage messageLogStorage, QueueMetadataStore queueMetadataStore) {
        this.messageLogStorage = Objects.requireNonNull(messageLogStorage, "messageLogStorage must not be null");
        this.queueMetadataStore = Objects.requireNonNull(queueMetadataStore, "queueMetadataStore must not be null");
    }

    @Override
    public QueuedMessage save(String queuePath, QueuedMessage message) {
        try {
            WriteResult result = messageLogStorage.append(queuePath, message);

            long msgId = message.getMessageId();
            idToRouteKey.put(msgId, new RouteKey(queuePath, result.queueOffset()));
            maxMessageId.updateAndGet(curr -> Math.max(curr, msgId));
            return message;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to persist message to AOL", e);
        }
    }

    @Override
    public void delete(long messageId) {
        RouteKey routeKey = idToRouteKey.remove(messageId);
        if (routeKey == null) return;

        try {
            messageLogStorage.acknowledge(routeKey.queuePath, routeKey.queueOffset);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to acknowledge message in AOL", e);
        }
    }

    @Override
    public void deleteByQueuePath(String queuePath) {
        var routeKeys = idToRouteKey.entrySet().stream()
                .filter(entry -> entry.getValue().queuePath.equals(queuePath))
                .map(Map.Entry::getValue)
                .toList();

        for (var routeKey : routeKeys) {
            try {
                messageLogStorage.acknowledge(routeKey.queuePath, routeKey.queueOffset);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to acknowledge message in AOL", e);
            }
        }
    }

    @Override
    public List<StoredMessageDto> loadAllPending() {
        List<StoredMessageDto> result = new ArrayList<>();

        for (var queueDto : queueMetadataStore.loadAll()) {
            String queuePath = queueDto.queuePath();

            try {
                var state = messageLogStorage.recover(queuePath);

                for (var msg : state.liveMessages()) {
                    long msgId = msg.getMessageId();
                    maxMessageId.updateAndGet(curr -> Math.max(curr, msgId));
                    result.add(new StoredMessageDto(queuePath, msg));
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Recovery failed for queue: " + queuePath, e);
            }
        }
        return result;
    }

    @Override
    public long findMaxMessageId() {
        long inFlightMax = idToRouteKey.keySet().stream()
                .mapToLong(Long::longValue)
                .max()
                .orElse(0L);
        return Math.max(inFlightMax, maxMessageId.get());
    }

    @Override
    public void close() throws IOException {
        messageLogStorage.close();
    }

    private record RouteKey(String queuePath, long queueOffset) {}
}
