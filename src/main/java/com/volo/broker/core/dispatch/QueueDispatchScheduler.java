package com.volo.broker.core.dispatch;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.volo.broker.domain.queue.QueueManager;

@Component
public class QueueDispatchScheduler {
    private final DispatchManager dispatchManager;
    private final QueueManager queueManager;
    private final ExecutorService executor;
    private final Set<String> dispatchingQueues = ConcurrentHashMap.newKeySet();

    public QueueDispatchScheduler(
            DispatchManager dispatchManager,
            QueueManager queueManager,
            @Value("${broker.dispatch.threads:2}") int threads
    ) {
        this.dispatchManager = dispatchManager;
        this.queueManager = queueManager;
        this.executor = Executors.newFixedThreadPool(Math.max(1, threads));
    }

    public void signal(String queueName){
        if(!dispatchingQueues.add(queueName)) return;

        executor.execute(() -> {
            try{
                dispatchManager.dispatchLoop(queueName);
            } finally {
                dispatchingQueues.remove(queueName);

                if(hasWork(queueName)){
                    signal(queueName);
                }
            }
        });
    }

    private boolean hasWork(String queueName){
        return queueManager.size(queueName) > 0;
    }

    public void shutdown() {
        executor.shutdown();
    }
}
