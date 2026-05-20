package com.volo.broker.transport;

import com.volo.broker.engine.QueueManager;
import com.volo.broker.engine.SubscriptionManager;
import com.volo.broker.engine.DispatchManager;
import com.volo.broker.engine.QueuedMessage;
import com.volo.broker.engine.DeliveryMode;
import com.volo.broker.transport.model.AckCommand;
import com.volo.broker.transport.model.PublishCommand;
import com.volo.broker.transport.model.PublishResult;
import com.volo.broker.transport.model.NackCommand;
import com.volo.broker.transport.model.SubscribeCommand;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class TransportCommandRouterImpl implements TransportCommandRouter {
    private final QueueManager queueManager;
    private final SubscriptionManager subscriptionManager;
    private final DispatchManager dispatchManager;
    private final AtomicLong idGen = new AtomicLong(1);

    public TransportCommandRouterImpl(QueueManager queueManager, SubscriptionManager subscriptionManager, DispatchManager dispatchManager) {
        this.queueManager = queueManager;
        this.subscriptionManager = subscriptionManager;
        this.dispatchManager = dispatchManager;
    }

    @Override
    public PublishResult route(PublishCommand cmd) {
        try {
            String queueName = cmd.routingKey == null || cmd.routingKey.isEmpty() ? "default" : cmd.routingKey;
            if (queueManager.getQueue(queueName).isEmpty()) queueManager.createQueue(queueName);
            long messageId = idGen.getAndIncrement();
            QueuedMessage qm = new QueuedMessage(messageId, cmd.payload, cmd.headers, cmd.routingKey, System.currentTimeMillis(), DeliveryMode.TRANSIENT);
            queueManager.enqueue(queueName, qm);
            // trigger dispatch for this queue
            dispatchManager.tryDispatch(queueName);
            return new PublishResult(true, messageId, null);
        } catch (Throwable t) {
            return new PublishResult(false, -1, t.getMessage());
        }
    }

    @Override
    public void route(SubscribeCommand cmd) {
        subscriptionManager.register(cmd.queueName, cmd.channel, cmd.prefetchCount);
        if (queueManager.getQueue(cmd.queueName).isEmpty()) queueManager.createQueue(cmd.queueName);
        dispatchManager.tryDispatch(cmd.queueName);
    }

    @Override
    public void route(AckCommand cmd) {
        dispatchManager.acknowledge(cmd.consumerTag, cmd.deliveryTag);
    }

    @Override
    public void route(NackCommand cmd) {
        dispatchManager.negativeAcknowledge(cmd.consumerTag, cmd.deliveryTag, cmd.requeue);
    }
}
