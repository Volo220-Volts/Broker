package com.volo.broker.broker.queue.settings;

public record FlowControlPolicy(
        boolean enabled,
        int highWatermarkMessages,
        int lowWatermarkMessages,
        long highWatermarkBytes,
        long lowWatermarkBytes,
        long maxBlockMs
) {
    public FlowControlPolicy {
        if (maxBlockMs < 0) {
            throw new IllegalArgumentException("maxBlockMs must not be negative");
        }

        if (enabled) {
            if (highWatermarkMessages < 0 || lowWatermarkMessages < 0) {
                throw new IllegalArgumentException("Message watermarks must not be negative");
            }
            if (highWatermarkBytes < 0 || lowWatermarkBytes < 0) {
                throw new IllegalArgumentException("Byte watermarks must not be negative");
            }
            if (highWatermarkMessages == 0 && highWatermarkBytes == 0) {
                throw new IllegalArgumentException("At least one high watermark (messages or bytes) must be > 0 when enabled");
            }
            if (highWatermarkMessages > 0 && lowWatermarkMessages >= highWatermarkMessages) {
                throw new IllegalArgumentException("lowWatermarkMessages must be strictly less than highWatermarkMessages");
            }
            if (highWatermarkBytes > 0 && lowWatermarkBytes >= highWatermarkBytes) {
                throw new IllegalArgumentException("lowWatermarkBytes must be strictly less than highWatermarkBytes");
            }
        }
    }

    public static FlowControlPolicy disabled() {
        return new FlowControlPolicy(false, 0, 0, 0, 0, 0);
    }

    public static FlowControlPolicy fromBrokerDefaults(
            int highWatermarkMessages,
            int lowWatermarkMessages,
            long highWatermarkBytes,
            long lowWatermarkBytes,
            long maxBlockMs
    ) {
        return new FlowControlPolicy(
                true,
                highWatermarkMessages,
                lowWatermarkMessages,
                highWatermarkBytes,
                lowWatermarkBytes,
                maxBlockMs
        );
    }
}

