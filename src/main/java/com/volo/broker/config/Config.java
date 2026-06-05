package com.volo.broker.config;

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

}
