package com.volo.broker.broker.queue;

import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.router.RouteTree;
import com.volo.broker.broker.queue.exception.QueueAlreadyExistsException;
import com.volo.broker.broker.queue.exception.QueueNotFoundException;
import com.volo.broker.broker.queue.settings.QueueSettings;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class InMemoryQueueManager implements QueueManager {
    private final ConcurrentMap<String, BrokerQueue> queues = new ConcurrentHashMap<>();
    private final RouteTree routeTree = new RouteTree();

    public RouteTree getRouteTree() {
        return routeTree;
    }

    @Override
    public BrokerQueue declare(QueuePath path, QueueSettings settings) {
        BrokerQueue queue = BrokerQueue.create(path, settings);

        BrokerQueue existing = queues.putIfAbsent(path.value(), queue);
        if (existing != null) {
            throw new QueueAlreadyExistsException("Queue already exists: " + path.value());
        }

        routeTree.attachQueue(path);

        return queue;
    }

    @Override
    public void delete(QueuePath path) {
        BrokerQueue removed = queues.remove(path.value());

        if (removed == null) {
            throw new QueueNotFoundException("Queue does not exist: " + path.value());
        }

        routeTree.detachQueue(path);
    }

    @Override
    public BrokerQueue require(String queueName) {
        return find(queueName)
                .orElseThrow(() -> new QueueNotFoundException("Queue does not exist: " + queueName));
    }

    @Override
    public Optional<BrokerQueue> find(String queueName) {
        if (queueName == null || queueName.isBlank()) {
            return Optional.empty();
        }

        return Optional.ofNullable(queues.get(queueName.trim()));
    }

    @Override
    public List<BrokerQueue> resolve(String routingKey) {
        return routeTree.resolve(routingKey)
                .stream()
                .map(this::require)
                .toList();
    }

    @Override
    public Collection<BrokerQueue> all() {
        return List.copyOf(queues.values());
    }
}
