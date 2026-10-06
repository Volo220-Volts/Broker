package com.volo.broker.persistence;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.persistence.aol.api.QueueStorage;
import com.volo.broker.persistence.aol.api.WriteResult;
import com.volo.broker.persistence.dto.StoredMessageDto;



/**
 * Serves as an adapter between the broker persistence layer and the append-only log system for message storage.
*/
public class AolMessageStore implements MessageStore {

    //Append only log storage for messages
    private final QueueStorage queueStorage;

    //Queue store for queue metadata
    private final QueueStore queueStore;

    private final Map<Long, RouteKey> idToRouteKey = new ConcurrentHashMap<>();


    public AolMessageStore(QueueStorage queueStorage, QueueStore queueStore) {
        this.queueStorage = queueStorage;
        this.queueStore = queueStore;
    }

    @Override
    public QueuedMessage save(String queuePath, QueuedMessage message) {
        try {
            WriteResult result = queueStorage.append(queuePath, message);

            idToRouteKey.put(message.getMessageId(), new RouteKey(queuePath, result.queueOffset()));
            return message;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to persist message to AOL", e);
        }
    }

    @Override
    public void delete(long messageId) {

        RouteKey routeKey = idToRouteKey.remove(messageId);
        if(routeKey == null) return;

        try {
            queueStorage.acknowledge(routeKey.queuePath, routeKey.queueOffset);
        } 
        catch (IOException e) {
            throw new UncheckedIOException("Failed to acknowledge message in AOL", e);
        }
    }

    @Override
    public void deleteByQueuePath(String queuePath) {
        var routeKeys = idToRouteKey.entrySet().stream()
                .filter(entry -> entry.getValue().queuePath.equals(queuePath))
                .map(Map.Entry::getValue)
                .toList();

        for(var routeKey : routeKeys) {
            try {
                queueStorage.acknowledge(routeKey.queuePath, routeKey.queueOffset);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to acknowledge message in AOL", e);
            }
        }
    }

    @Override
    public List<StoredMessageDto> loadAllPending() {
        List<StoredMessageDto> result = new ArrayList<>();

        for(var queueDto : queueStore.loadAll()){
            String queuePath = queueDto.queuePath();
        
            try {
                var state = queueStorage.recover(queuePath);

                for(var msg : state.liveMessages()){
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
        return idToRouteKey.keySet().stream()
                .mapToLong(Long::longValue)
                .max()
                .orElse(0L);
    }


    private record RouteKey(String queuePath, long queueOffset) {}

}
