package com.volo.broker.broker.dispatch;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.stereotype.Component;

import com.volo.broker.broker.queue.QueueManager;

@Component
public class QueueDispatchScheduler {
    private final DispatchManager dispatchManager;
    private final QueueManager queueManager;
    private final ExecutorService executor;
    private final Set<String> dispatchingQueues = ConcurrentHashMap.newKeySet();

    public QueueDispatchScheduler(
            DispatchManager dispatchManager,
            QueueManager queueManager
    ) {
        this.dispatchManager = dispatchManager;
        this.queueManager = queueManager;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
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
        return queueManager.find(queueName)
            .map(q -> q.size() > 0)
            .orElse(false);
    }

    public void shutdown() {
        executor.shutdown();
    }
}
