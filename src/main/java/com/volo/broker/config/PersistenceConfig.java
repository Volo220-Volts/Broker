package com.volo.broker.config;

import java.io.IOException;
import java.nio.file.Path;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.volo.broker.persistence.AolMessageStore;
import com.volo.broker.persistence.MessageStore;
import com.volo.broker.persistence.QueueStore;
import com.volo.broker.persistence.aol.api.QueueStorage;
import com.volo.broker.persistence.aol.store.AolQueueStorage;


@Configuration 
public class PersistenceConfig {

    @Value("${broker.aol.base-dir:data/aol}")
    private Path baseDir;

    @Bean 
    public QueueStorage aolQueueStorage() throws IOException{
        return new AolQueueStorage(baseDir);
    }

    @Bean
    @Primary 
    public MessageStore aolMessageStore(QueueStorage aolQueueStorage, QueueStore queueStore) {
        return new AolMessageStore(aolQueueStorage, queueStore);
    }
}
