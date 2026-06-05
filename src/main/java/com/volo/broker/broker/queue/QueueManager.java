package com.volo.broker.broker.queue;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.volo.broker.broker.queue.settings.QueueSettings;
import com.volo.broker.broker.router.QueuePath;

public interface QueueManager {

    BrokerQueue declare(QueuePath path, QueueSettings settings);

    void delete(QueuePath path);

    BrokerQueue require(String queuePaht);

    Optional<BrokerQueue> find(String queuePath);

    List<BrokerQueue> resolve(String routingKey);

    Collection<BrokerQueue> all();
}
