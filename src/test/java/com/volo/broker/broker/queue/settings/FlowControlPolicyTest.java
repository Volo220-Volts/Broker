package com.volo.broker.broker.queue.settings;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlowControlPolicyTest {

    @Test
    void disabled_shouldCreateValidDisabledPolicy() {
        var policy = FlowControlPolicy.disabled();
        assertThat(policy.enabled()).isFalse();
        assertThat(policy.highWatermarkMessages()).isZero();
        assertThat(policy.lowWatermarkMessages()).isZero();
    }

    @Test
    void validEnabledPolicy_shouldSucceed() {
        var policy = new FlowControlPolicy(true, 100, 50, 1024, 512, 5000);
        assertThat(policy.enabled()).isTrue();
        assertThat(policy.highWatermarkMessages()).isEqualTo(100);
        assertThat(policy.lowWatermarkMessages()).isEqualTo(50);
        assertThat(policy.highWatermarkBytes()).isEqualTo(1024);
        assertThat(policy.lowWatermarkBytes()).isEqualTo(512);
        assertThat(policy.maxBlockMs()).isEqualTo(5000);
    }

    @Test
    void enabledWithBothHighWatermarksZero_shouldThrow() {
        assertThatThrownBy(() -> new FlowControlPolicy(true, 0, 0, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("At least one high watermark");
    }

    @Test
    void enabledWithLowMessageGreaterOrEqualHigh_shouldThrow() {
        assertThatThrownBy(() -> new FlowControlPolicy(true, 100, 100, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lowWatermarkMessages must be strictly less than highWatermarkMessages");

        assertThatThrownBy(() -> new FlowControlPolicy(true, 100, 101, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lowWatermarkMessages must be strictly less than highWatermarkMessages");
    }

    @Test
    void enabledWithLowBytesGreaterOrEqualHigh_shouldThrow() {
        assertThatThrownBy(() -> new FlowControlPolicy(true, 0, 0, 1000, 1000, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lowWatermarkBytes must be strictly less than highWatermarkBytes");
    }

    @Test
    void negativeMaxBlockMs_shouldThrow() {
        assertThatThrownBy(() -> new FlowControlPolicy(false, 0, 0, 0, 0, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBlockMs must not be negative");
    }
}

