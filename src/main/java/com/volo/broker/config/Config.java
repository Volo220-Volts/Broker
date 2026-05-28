package com.volo.broker.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.volo.broker.domain.queue.InMemoryDequeManager;
import com.volo.broker.domain.queue.QueueManager;

@Configuration
public class Config {
    
    @Bean
    public QueueManager queueManager() {
        return new InMemoryDequeManager();
    }

}
