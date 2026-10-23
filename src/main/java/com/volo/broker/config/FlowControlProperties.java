package com.volo.broker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "broker.flow-control")
public record FlowControlProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue Defaults defaults,
        @DefaultValue Global global,
        @DefaultValue("64") int resumeBatch
) {
    public FlowControlProperties {
        if (defaults == null) {
            defaults = new Defaults(10000, 5000, 67108864L, 33554432L, 0L);
        }
        if (global == null) {
            global = new Global(1073741824L, 805306368L);
        }
        if (resumeBatch <= 0) {
            resumeBatch = 64;
        }
    }

    public record Defaults(
            @DefaultValue("10000") int highWatermarkMessages,
            @DefaultValue("5000") int lowWatermarkMessages,
            @DefaultValue("67108864") long highWatermarkBytes,
            @DefaultValue("33554432") long lowWatermarkBytes,
            @DefaultValue("0") long maxBlockMs
    ) {
        public Defaults {
            if (highWatermarkMessages < 0) highWatermarkMessages = 10000;
            if (lowWatermarkMessages < 0) lowWatermarkMessages = 5000;
            if (highWatermarkBytes < 0) highWatermarkBytes = 67108864L;
            if (lowWatermarkBytes < 0) lowWatermarkBytes = 33554432L;
            if (maxBlockMs < 0) maxBlockMs = 0L;
        }
    }

    public record Global(
            @DefaultValue("1073741824") long highBytes,
            @DefaultValue("805306368") long lowBytes
    ) {
        public Global {
            if (highBytes <= 0) highBytes = 1073741824L;
            if (lowBytes <= 0) lowBytes = 805306368L;
        }
    }
}

