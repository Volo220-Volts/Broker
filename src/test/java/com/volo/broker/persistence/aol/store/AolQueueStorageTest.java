package com.volo.broker.persistence.aol.store;

import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.persistence.aol.api.RecoveryResult;
import com.volo.broker.persistence.aol.api.WriteResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AolQueueStorageTest {

    @TempDir
    Path tempDir;

    // ======================================================================
    // Append
    // ======================================================================

    @Nested
    @DisplayName("append")
    class Append {

        @Test
        void offsets_are_assigned_monotonically_starting_from_zero() throws IOException {
            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                WriteResult r0 = storage.append("q", msg(1, "A"));
                WriteResult r1 = storage.append("q", msg(2, "B"));
                WriteResult r2 = storage.append("q", msg(3, "C"));

                assertThat(r0.queueOffset()).isZero();
                assertThat(r1.queueOffset()).isEqualTo(1L);
                assertThat(r2.queueOffset()).isEqualTo(2L);
            }
        }

        @Test
        void physical_positions_are_strictly_increasing() throws IOException {
            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                WriteResult r0 = storage.append("q", msg(1, "hello"));
                WriteResult r1 = storage.append("q", msg(2, "world"));

                assertThat(r0.physicalPosition()).isLessThan(r1.physicalPosition());
            }
        }
    }

    // ======================================================================
    // Recovery — basic
    // ======================================================================

    @Nested
    @DisplayName("recover")
    class Recover {

        @Test
        void empty_queue_returns_empty_recovered_state() throws IOException {
            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                RecoveryResult state = storage.recover("no.such.queue");

                assertThat(state.isEmpty()).isTrue();
                assertThat(state.nextQueueOffset()).isZero();
            }
        }

        @Test
        void all_unacked_messages_are_returned_as_redelivered_after_restart() throws IOException {
            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                storage.append("q.orders", msg(10, "A"));
                storage.append("q.orders", msg(11, "B"));
                storage.append("q.orders", msg(12, "C"));
            }

            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                RecoveryResult state = storage.recover("q.orders");

                assertThat(state.liveMessages()).hasSize(3);
                assertThat(state.nextQueueOffset()).isEqualTo(3L);
                assertThat(state.liveMessages()).allMatch(QueuedMessage::isRedelivered);
            }
        }

        @Test
        void acknowledged_messages_are_excluded_from_recovery() throws IOException {
            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                WriteResult r0 = storage.append("q", msg(1, "A")); // offset 0
                storage.append("q", msg(2, "B"));                  // offset 1
                WriteResult r2 = storage.append("q", msg(3, "C")); // offset 2
                storage.append("q", msg(4, "D"));                  // offset 3

                storage.acknowledge("q", r0.queueOffset()); // ack 0
                storage.acknowledge("q", r2.queueOffset()); // ack 2
            }

            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                RecoveryResult state = storage.recover("q");

                assertThat(state.liveMessages()).hasSize(2);
                assertThat(state.liveMessages())
                        .extracting(QueuedMessage::getMessageId)
                        .containsExactlyInAnyOrder(2L, 4L);
            }
        }

        @Test
        void next_queue_offset_equals_max_written_offset_plus_one() throws IOException {
            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                for (int i = 0; i < 5; i++) storage.append("q.pay", msg(i + 100L, "p-" + i));
                storage.acknowledge("q.pay", 0L);
                storage.acknowledge("q.pay", 2L);
                storage.acknowledge("q.pay", 4L);
            }

            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                RecoveryResult state = storage.recover("q.pay");

                assertThat(state.liveMessages()).hasSize(2);
                assertThat(state.nextQueueOffset()).isEqualTo(5L); // max offset was 4
            }
        }
    }

    // ======================================================================
    // Multi-queue isolation
    // ======================================================================

    @Nested
    @DisplayName("queue isolation")
    class QueueIsolation {

        @Test
        void two_queues_store_and_recover_independently() throws IOException {
            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                storage.append("q.alpha", msg(1, "from-alpha"));
                storage.append("q.beta",  msg(2, "from-beta"));
                storage.append("q.alpha", msg(3, "from-alpha-2"));
                storage.acknowledge("q.alpha", 0L); // ack first alpha message
            }

            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                RecoveryResult alpha = storage.recover("q.alpha");
                RecoveryResult beta  = storage.recover("q.beta");

                assertThat(alpha.liveMessages()).hasSize(1);
                assertThat(beta.liveMessages()).hasSize(1);
                assertThat(alpha.nextQueueOffset()).isEqualTo(2L); // 2 appends: offsets 0,1
                assertThat(beta.nextQueueOffset()).isEqualTo(1L);  // 1 append: offset 0
            }
        }
    }

    // ======================================================================
    // Crash recovery
    // ======================================================================

    @Nested
    @DisplayName("crash recovery")
    class CrashRecovery {

        @Test
        void corrupt_tail_is_repaired_and_valid_records_are_returned() throws IOException {
            // AolQueueStorage encodes '.' as-is, '/' as '_'
            // "crash.q" → directory name "crash.q"
            Path dataLog = tempDir
                    .resolve("queues")
                    .resolve("crash.q")
                    .resolve("data.log");

            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                storage.append("crash.q", msg(1, "ok-1"));
                storage.append("crash.q", msg(2, "ok-2"));
            }

            // Simulate crash: append 20 garbage bytes
            Files.write(dataLog, new byte[20], StandardOpenOption.APPEND);

            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                RecoveryResult state = storage.recover("crash.q");

                assertThat(state.liveMessages()).hasSize(2);
                // File must have been truncated (garbage removed)
                assertThat(Files.size(dataLog)).isLessThan(10_000L);
            }
        }
    }

    // ======================================================================
    // Append after recover
    // ======================================================================

    @Nested
    @DisplayName("append after recover")
    class AppendAfterRecover {

        @Test
        void offsets_continue_from_next_queue_offset_after_restart() throws IOException {
            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                storage.append("q", msg(1, "A"));
                storage.append("q", msg(2, "B"));
                storage.append("q", msg(3, "C"));
            }

            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                RecoveryResult state = storage.recover("q");
                assertThat(state.nextQueueOffset()).isEqualTo(3L);

                WriteResult r3 = storage.append("q", msg(4, "D"));
                WriteResult r4 = storage.append("q", msg(5, "E"));

                assertThat(r3.queueOffset()).isEqualTo(3L);
                assertThat(r4.queueOffset()).isEqualTo(4L);
            }
        }

        @Test
        void all_messages_across_sessions_are_returned_on_subsequent_recovery() throws IOException {
            // Session 1: write 3
            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                storage.append("q", msg(1, "A"));
                storage.append("q", msg(2, "B"));
                storage.append("q", msg(3, "C"));
            }

            // Session 2: recover + write 2 more
            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                storage.recover("q");
                storage.append("q", msg(4, "D"));
                storage.append("q", msg(5, "E"));
            }

            // Session 3: recover again — all 5 should be present
            try (AolQueueStorage storage = new AolQueueStorage(tempDir)) {
                RecoveryResult state = storage.recover("q");

                assertThat(state.liveMessages()).hasSize(5);
                assertThat(state.nextQueueOffset()).isEqualTo(5L);
            }
        }
    }

    // ======================================================================
    // Helpers
    // ======================================================================

    private static QueuedMessage msg(long id, String payload) {
        return new QueuedMessage(id, payload.getBytes(), Map.of(),
                "routing.key", 1_700_000_000_000L, false, 0);
    }
}
