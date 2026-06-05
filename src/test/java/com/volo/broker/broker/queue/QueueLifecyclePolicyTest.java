package com.volo.broker.broker.queue;

import com.volo.broker.broker.queue.settings.QueueLifecyclePolicy;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class QueueLifecyclePolicyTest {

    @Test
    void defaults_shouldDisableAutoDeleteAndIdleTimeout() {
        QueueLifecyclePolicy policy = QueueLifecyclePolicy.defaults();

        assertThat(policy.autoDelete()).isFalse();
        assertThat(policy.idleTimeoutEnabled()).isFalse();
        assertThat(policy.idleTimeoutMs()).isZero();
    }

    @Test
    void constructor_shouldRejectEnabledIdleTimeoutWithInvalidValue() {
        assertThatThrownBy(() -> new QueueLifecyclePolicy(
                false,
                true,
                0,
                false
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("idleTimeoutMs");
    }

    @Test
    void constructor_shouldAllowDisabledIdleTimeoutWithZeroValue() {
        QueueLifecyclePolicy policy = new QueueLifecyclePolicy(
                false,
                false,
                0,
                false
        );

        assertThat(policy.idleTimeoutEnabled()).isFalse();
        assertThat(policy.idleTimeoutMs()).isZero();
    }

    @Test
    void constructor_shouldAllowAutoDelete() {
        QueueLifecyclePolicy policy = new QueueLifecyclePolicy(
                true,
                false,
                0,
                false
        );

        assertThat(policy.autoDelete()).isTrue();
    }
}