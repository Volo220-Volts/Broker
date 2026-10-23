package com.volo.broker.broker.queue;

import com.volo.broker.TestMessages;
import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.flow.FlowState;
import com.volo.broker.broker.flow.FlowTransition;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.settings.FlowControlPolicy;
import com.volo.broker.broker.router.QueuePath;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerQueueFlowTest {

    @Test
    @DisplayName("enqueue transitions to BLOCKED at high watermark messages, poll transitions to OPEN at low watermark")
    void watermarkTransitions_byMessageCount() {
        FlowControlPolicy policy = new FlowControlPolicy(true, 5, 2, 0, 0, 0);
        BrokerQueue queue = BrokerQueue.create(QueuePath.of("test.queue"), TestQueueSettings.withFlowControl(policy));

        assertThat(queue.flowState()).isEqualTo(FlowState.OPEN);

        for (int i = 1; i <= 4; i++) {
            EnqueueResult res = queue.enqueue(TestMessages.message(i));
            assertThat(res.accepted()).isTrue();
            assertThat(res.transition()).isEqualTo(FlowTransition.NONE);
            assertThat(queue.flowState()).isEqualTo(FlowState.OPEN);
        }

        // 5th message hits HWM (5) -> transition to BLOCKED
        EnqueueResult res5 = queue.enqueue(TestMessages.message(5));
        assertThat(res5.transition()).isEqualTo(FlowTransition.TO_BLOCKED);
        assertThat(queue.flowState()).isEqualTo(FlowState.BLOCKED);

        // 6th message keeps BLOCKED, transition NONE
        EnqueueResult res6 = queue.enqueue(TestMessages.message(6));
        assertThat(res6.transition()).isEqualTo(FlowTransition.NONE);
        assertThat(queue.flowState()).isEqualTo(FlowState.BLOCKED);

        // Poll messages 1, 2, 3 -> sizes become 5, 4, 3 (all > LWM 2)
        for (int i = 0; i < 3; i++) {
            PollResult pollRes = queue.poll();
            assertThat(pollRes.message()).isPresent();
            assertThat(pollRes.transition()).isEqualTo(FlowTransition.NONE);
            assertThat(queue.flowState()).isEqualTo(FlowState.BLOCKED);
        }

        // Current size is 3. Polling one more drops size to 2 (<= LWM 2) -> transition to OPEN
        PollResult unblockPoll = queue.poll();
        assertThat(unblockPoll.message()).isPresent();
        assertThat(unblockPoll.transition()).isEqualTo(FlowTransition.TO_OPEN);
        assertThat(queue.flowState()).isEqualTo(FlowState.OPEN);

        // Polling down to 1 keeps OPEN, transition NONE
        PollResult furtherPoll = queue.poll();
        assertThat(furtherPoll.message()).isPresent();
        assertThat(furtherPoll.transition()).isEqualTo(FlowTransition.NONE);
        assertThat(queue.flowState()).isEqualTo(FlowState.OPEN);
    }

    @Test
    @DisplayName("enqueue transitions to BLOCKED when byte watermark is exceeded")
    void watermarkTransitions_byByteCount() {
        // High = 200 bytes, Low = 100 bytes
        FlowControlPolicy policy = new FlowControlPolicy(true, 1000, 500, 200, 100, 0);
        BrokerQueue queue = BrokerQueue.create(QueuePath.of("test.bytes"), TestQueueSettings.withFlowControl(policy));

        byte[] payload120Bytes = new byte[120];
        QueuedMessage msg1 = new QueuedMessage(1L, payload120Bytes, Map.of(), "test.bytes", System.currentTimeMillis(), false, 0);
        QueuedMessage msg2 = new QueuedMessage(2L, payload120Bytes, Map.of(), "test.bytes", System.currentTimeMillis(), false, 0);

        EnqueueResult res1 = queue.enqueue(msg1);
        assertThat(res1.transition()).isEqualTo(FlowTransition.NONE);
        assertThat(queue.flowState()).isEqualTo(FlowState.OPEN);

        // 2nd message pushes total bytes to ~240+ (> 200 HWM)
        EnqueueResult res2 = queue.enqueue(msg2);
        assertThat(res2.transition()).isEqualTo(FlowTransition.TO_BLOCKED);
        assertThat(queue.flowState()).isEqualTo(FlowState.BLOCKED);

        // Poll 1st message -> remaining bytes drops back to ~120 (which is > 100 LWM)
        PollResult p1 = queue.poll();
        assertThat(p1.transition()).isEqualTo(FlowTransition.NONE);
        assertThat(queue.flowState()).isEqualTo(FlowState.BLOCKED);

        // Poll 2nd message -> remaining bytes is 0 (<= 100 LWM) -> TO_OPEN
        PollResult p2 = queue.poll();
        assertThat(p2.transition()).isEqualTo(FlowTransition.TO_OPEN);
        assertThat(queue.flowState()).isEqualTo(FlowState.OPEN);
    }

    @Test
    @DisplayName("requeue triggers transition to BLOCKED when queue fills up")
    void requeue_triggersTransitionToBlocked() {
        FlowControlPolicy policy = new FlowControlPolicy(true, 2, 1, 0, 0, 0);
        BrokerQueue queue = BrokerQueue.create(QueuePath.of("test.requeue"), TestQueueSettings.withFlowControl(policy));

        queue.enqueue(TestMessages.message(1));
        assertThat(queue.flowState()).isEqualTo(FlowState.OPEN);

        FlowTransition transition = queue.requeue(TestMessages.message(2));
        assertThat(transition).isEqualTo(FlowTransition.TO_BLOCKED);
        assertThat(queue.flowState()).isEqualTo(FlowState.BLOCKED);
    }

    @Test
    @DisplayName("disabled flow control never transitions to BLOCKED")
    void disabledFlowControl_neverBlocks() {
        BrokerQueue queue = BrokerQueue.create(QueuePath.of("test.disabled"), TestQueueSettings.reliable());

        for (int i = 1; i <= 20; i++) {
            EnqueueResult res = queue.enqueue(TestMessages.message(i));
            assertThat(res.transition()).isEqualTo(FlowTransition.NONE);
            assertThat(queue.flowState()).isEqualTo(FlowState.OPEN);
        }
    }
}

