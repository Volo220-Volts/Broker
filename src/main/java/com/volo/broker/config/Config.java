package com.volo.broker.config;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManager;

@Configuration
public class Config {
    
    @Bean
    public QueueManager queueManager() {
        return new InMemoryQueueManager();
    }

    @Bean(destroyMethod = "shutdownNow")
    public ScheduledExecutorService backgroundTaskExecutor() {
        return Executors.newScheduledThreadPool(Math.max(1, Runtime.getRuntime().availableProcessors() / 2));
    }
}
