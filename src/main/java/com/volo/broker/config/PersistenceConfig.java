package com.volo.broker.config;

import java.io.IOException;
import java.nio.file.Path;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.volo.broker.persistence.AolMessageStore;
import com.volo.broker.persistence.FileQueueMetadataStore;
import com.volo.broker.persistence.MessageStore;
import com.volo.broker.persistence.QueueMetadataStore;
import com.volo.broker.persistence.aol.api.MessageLogStorage;
import com.volo.broker.persistence.aol.engine.BatchConfig;
import com.volo.broker.persistence.aol.engine.FsyncMode;
import com.volo.broker.persistence.aol.store.AolMessageLogStorage;

@Configuration 
public class PersistenceConfig {

    @Value("${broker.aol.base-dir:data/aol}")
    private Path baseDir;

    @Value("${broker.aol.sync-ack:false}")
    private boolean syncAck;

    @Value("${broker.aol.max-batch-size:256}")
    private int batchSize;

    @Value("${broker.aol.linger-micros:1500}")
    private long lingerMicros;

    @Value("${broker.aol.ack-batch-size:512}")
    private int ackBatchSize;

    @Value("${broker.aol.ack-linger-micros:4000}")
    private long ackLingerMicros;

    @Bean
    public BatchConfig batchConfig() {
        return new BatchConfig(batchSize, lingerMicros);
    }

    @Bean
    public BatchConfig ackBatchConfig() {
        return new BatchConfig(ackBatchSize, ackLingerMicros);
    }

    @Bean
    public QueueMetadataStore queueMetadataStore() {
        return new FileQueueMetadataStore(baseDir);
    }

    @Bean(destroyMethod = "close") 
    public MessageLogStorage messageLogStorage(
            @Qualifier("batchConfig") BatchConfig batchConfig,
            @Qualifier("ackBatchConfig") BatchConfig ackBatchConfig
    ) throws IOException {
        return new AolMessageLogStorage(baseDir, syncAck, batchConfig, ackBatchConfig, FsyncMode.PARALLEL);
    }

    @Bean(destroyMethod = "close")
    public MessageStore messageStore(MessageLogStorage messageLogStorage, QueueMetadataStore queueMetadataStore) {
        return new AolMessageStore(messageLogStorage, queueMetadataStore);
    }
}
