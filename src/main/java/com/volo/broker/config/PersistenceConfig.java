package com.volo.broker.config;

import java.io.IOException;
import java.nio.file.Path;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.volo.broker.persistence.AolMessageStore;
import com.volo.broker.persistence.FileQueueMetadataStore;
import com.volo.broker.persistence.MessageStore;
import com.volo.broker.persistence.QueueMetadataStore;
import com.volo.broker.persistence.aol.api.MessageLogStorage;
import com.volo.broker.persistence.aol.store.AolMessageLogStorage;

@Configuration 
public class PersistenceConfig {

    @Value("${broker.aol.base-dir:data/aol}")
    private Path baseDir;

    @Bean
    public QueueMetadataStore queueMetadataStore() {
        return new FileQueueMetadataStore(baseDir);
    }

    @Bean(destroyMethod = "close") 
    public MessageLogStorage messageLogStorage() throws IOException {
        return new AolMessageLogStorage(baseDir);
    }

    @Bean(destroyMethod = "close")
    public MessageStore messageStore(MessageLogStorage messageLogStorage, QueueMetadataStore queueMetadataStore) {
        return new AolMessageStore(messageLogStorage, queueMetadataStore);
    }
}
