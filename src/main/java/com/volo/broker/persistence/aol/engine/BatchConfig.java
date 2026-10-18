package com.volo.broker.persistence.aol.engine;

public record BatchConfig(
    int maxBatchSize,
    long lingerMicros
) {
    public static final BatchConfig DEFAULT_DATA = new BatchConfig(256, 1500L);
    public static final BatchConfig DEFAULT_ACK = new BatchConfig(512, 4000L);

    public BatchConfig {
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be > 0");
        }
        if (lingerMicros < 0) {
            throw new IllegalArgumentException("lingerMicros must be >= 0");
        }
    }
}
