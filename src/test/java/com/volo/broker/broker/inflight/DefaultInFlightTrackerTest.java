package com.volo.broker.broker.inflight;

import com.volo.broker.TestMessages;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultInFlightTrackerTest {

    @Test
    void track_shouldTrackMessageByDeliveryTag() {
        DefaultInFlightTracker tracker = new DefaultInFlightTracker();

        tracker.track(1L, TestMessages.message(100));

        assertThat(tracker.size()).isEqualTo(1);
    }

    @Test
    void remove_shouldReturnTrackedMessage() {
        DefaultInFlightTracker tracker = new DefaultInFlightTracker();

        tracker.track(1L, TestMessages.message(100));

        var removed = tracker.remove(1L);

        assertThat(removed).isNotNull();
        assertThat(removed.getMessageId()).isEqualTo(100L);
        assertThat(tracker.size()).isZero();
    }

    @Test
    void remove_shouldReturnNullForUnknownDeliveryTag() {
        DefaultInFlightTracker tracker = new DefaultInFlightTracker();

        var removed = tracker.remove(404L);

        assertThat(removed).isNull();
    }

    @Test
    void drainAll_shouldReturnMessagesInDeliveryTagOrderAndClearTracker() {
        DefaultInFlightTracker tracker = new DefaultInFlightTracker();

        tracker.track(2L, TestMessages.message(200));
        tracker.track(1L, TestMessages.message(100));
        tracker.track(3L, TestMessages.message(300));

        var drained = tracker.drainAll();

        assertThat(drained)
                .extracting("messageId")
                .containsExactly(100L, 200L, 300L);

        assertThat(tracker.size()).isZero();
    }
}