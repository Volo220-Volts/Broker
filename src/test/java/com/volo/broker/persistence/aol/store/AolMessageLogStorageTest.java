package com.volo.broker.persistence.aol.store;

import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.persistence.aol.api.RecoveryResult;
import com.volo.broker.persistence.aol.api.WriteResult;
import com.volo.broker.persistence.aol.engine.FsyncMode;

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

class AolMessageLogStorageTest {

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
            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
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
            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
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
            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                RecoveryResult state = storage.recover("no.such.queue");

                assertThat(state.isEmpty()).isTrue();
                assertThat(state.nextQueueOffset()).isZero();
            }
        }

        @Test
        void all_unacked_messages_are_returned_as_redelivered_after_restart() throws IOException {
            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                storage.append("q.orders", msg(10, "A"));
                storage.append("q.orders", msg(11, "B"));
                storage.append("q.orders", msg(12, "C"));
            }

            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                RecoveryResult state = storage.recover("q.orders");

                assertThat(state.liveMessages()).hasSize(3);
                assertThat(state.nextQueueOffset()).isEqualTo(3L);
                assertThat(state.liveMessages()).allMatch(QueuedMessage::isRedelivered);
            }
        }

        @Test
        void acknowledged_messages_are_excluded_from_recovery() throws IOException {
            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                WriteResult r0 = storage.append("q", msg(1, "A")); // offset 0
                storage.append("q", msg(2, "B"));                  // offset 1
                WriteResult r2 = storage.append("q", msg(3, "C")); // offset 2
                storage.append("q", msg(4, "D"));                  // offset 3

                storage.acknowledge("q", r0.queueOffset()); // ack 0
                storage.acknowledge("q", r2.queueOffset()); // ack 2
            }

            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                RecoveryResult state = storage.recover("q");

                assertThat(state.liveMessages()).hasSize(2);
                assertThat(state.liveMessages())
                        .extracting(QueuedMessage::getMessageId)
                        .containsExactly(2L, 4L);
                assertThat(state.nextQueueOffset()).isEqualTo(4L);
            }
        }

        @Test
        void next_queue_offset_is_preserved_when_all_messages_are_acknowledged() throws IOException {
            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                WriteResult r0 = storage.append("q", msg(1, "A"));
                WriteResult r1 = storage.append("q", msg(2, "B"));

                storage.acknowledge("q", r0.queueOffset());
                storage.acknowledge("q", r1.queueOffset());
            }

            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                RecoveryResult state = storage.recover("q");

                assertThat(state.liveMessages()).isEmpty();
                assertThat(state.nextQueueOffset()).isEqualTo(2L);
            }
        }

        @Test
        void recovery_after_restart_preserves_monotonic_offset_continuity() throws IOException {
            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                storage.append("q", msg(1, "first"));
                storage.append("q", msg(2, "second"));
            }

            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                RecoveryResult state = storage.recover("q");
                assertThat(state.nextQueueOffset()).isEqualTo(2L);

                WriteResult r2 = storage.append("q", msg(3, "third"));
                assertThat(r2.queueOffset()).isEqualTo(2L);
            }
        }
    }

    // ======================================================================
    // Recovery — corrupted tail truncation
    // ======================================================================

    @Nested
    @DisplayName("recover with tail corruption")
    class TailCorruption {

        @Test
        void corrupt_trailing_bytes_are_truncated_and_valid_prefix_is_recovered()
                throws IOException {
            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                storage.append("q.corrupt", msg(1, "good-1"));
                storage.append("q.corrupt", msg(2, "good-2"));
            }

            Path dataLog = tempDir.resolve("queues")
                    .resolve("q.corrupt")
                    .resolve("data.log");

            byte[] garbage = new byte[]{0x00, 0x00, 0x00, 0x20, 0x7F, 0x7F, 0x7F, 0x7F};
            Files.write(dataLog, garbage, StandardOpenOption.APPEND);

            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                RecoveryResult state = storage.recover("q.corrupt");

                assertThat(state.liveMessages()).hasSize(2);
                assertThat(state.nextQueueOffset()).isEqualTo(2L);

                WriteResult r2 = storage.append("q.corrupt", msg(3, "good-3"));
                assertThat(r2.queueOffset()).isEqualTo(2L);
            }
        }
    }

    // ======================================================================
    // Multi-queue isolation
    // ======================================================================

    @Nested
    @DisplayName("multi-queue isolation")
    class MultiQueueIsolation {

        @Test
        void independent_queues_have_independent_offset_sequences() throws IOException {
            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                WriteResult q1_r0 = storage.append("q1", msg(1, "A"));
                WriteResult q2_r0 = storage.append("q2", msg(2, "B"));

                assertThat(q1_r0.queueOffset()).isZero();
                assertThat(q2_r0.queueOffset()).isZero();
            }
        }

        @Test
        void acks_on_one_queue_do_not_affect_another() throws IOException {
            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                WriteResult rA = storage.append("qA", msg(1, "A"));
                storage.append("qB", msg(2, "B"));

                storage.acknowledge("qA", rA.queueOffset());
            }

            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                RecoveryResult stateA = storage.recover("qA");
                RecoveryResult stateB = storage.recover("qB");

                assertThat(stateA.liveMessages()).isEmpty();
                assertThat(stateB.liveMessages()).hasSize(1);
            }
        }

        @Test
        void hierarchical_and_dotted_queue_paths_create_isolated_directories()
                throws IOException {
            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                storage.append("finance.payments", msg(1, "pay"));
                storage.append("finance/invoices", msg(2, "inv"));
            }

            Path queuesRoot = tempDir.resolve("queues");
            assertThat(Files.isDirectory(queuesRoot.resolve("finance.payments"))).isTrue();
            assertThat(Files.isDirectory(queuesRoot.resolve("finance_invoices"))).isTrue();

            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                assertThat(storage.recover("finance.payments").liveMessages()).hasSize(1);
                assertThat(storage.recover("finance/invoices").liveMessages()).hasSize(1);
            }
        }
    }

    // ======================================================================
    // Engine & Fsync Modes
    // ======================================================================

    @Nested
    @DisplayName("engine & fsync modes")
    class EngineAndFsyncModes {

        @Test
        void parallel_fsync_mode_appends_and_recovers_correctly() throws IOException {
            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false, FsyncMode.PARALLEL)) {
                assertThat(storage.getFsyncMode()).isEqualTo(com.volo.broker.persistence.aol.engine.FsyncMode.PARALLEL);

                storage.append("q.par", msg(1, "msg1"));
                storage.append("q.par", msg(2, "msg2"));
                storage.acknowledge("q.par", 0L);
            }

            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false, FsyncMode.PARALLEL)) {
                RecoveryResult res = storage.recover("q.par");
                assertThat(res.liveMessages()).hasSize(1);
                assertThat(res.liveMessages().get(0).getMessageId()).isEqualTo(2L);
            }
        }

        @Test
        void dynamic_switching_between_sequential_and_parallel_fsync() throws IOException {
            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false, FsyncMode.SEQUENTIAL)) {
                assertThat(storage.getFsyncMode()).isEqualTo(com.volo.broker.persistence.aol.engine.FsyncMode.SEQUENTIAL);

                storage.append("q.dyn", msg(1, "first"));

                storage.setFsyncMode(com.volo.broker.persistence.aol.engine.FsyncMode.PARALLEL);
                assertThat(storage.getFsyncMode()).isEqualTo(com.volo.broker.persistence.aol.engine.FsyncMode.PARALLEL);
                storage.append("q.dyn", msg(2, "second"));

                storage.setFsyncMode(com.volo.broker.persistence.aol.engine.FsyncMode.SEQUENTIAL);
                assertThat(storage.getFsyncMode()).isEqualTo(com.volo.broker.persistence.aol.engine.FsyncMode.SEQUENTIAL);
                storage.append("q.dyn", msg(3, "third"));
            }

            try (AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, false)) {
                RecoveryResult res = storage.recover("q.dyn");
                assertThat(res.liveMessages()).hasSize(3);
            }
        }

        @Test
        void mpsc_engine_mode_appends_and_recovers_correctly() throws IOException {
            try (AolMessageLogStorage storage = new AolMessageLogStorage(
                    tempDir,
                    false,
                    FsyncMode.PARALLEL)) {
                assertThat(storage.getFsyncMode()).isEqualTo(com.volo.broker.persistence.aol.engine.FsyncMode.PARALLEL);
                assertThat(storage.getMpscEngine()).isNotNull();

                storage.append("q.mpsc", msg(1, "mpsc1"));
                storage.append("q.mpsc", msg(2, "mpsc2"));
                storage.acknowledge("q.mpsc", 0L);
            }

            try (AolMessageLogStorage storage = new AolMessageLogStorage(
                    tempDir,
                    false,
                    FsyncMode.PARALLEL)) {
                RecoveryResult res = storage.recover("q.mpsc");
                assertThat(res.liveMessages()).hasSize(1);
                assertThat(res.liveMessages().get(0).getMessageId()).isEqualTo(2L);
            }
        }

        @Test
        void fsync_statistics_track_data_and_ack_forces_separately() throws IOException {
            AolMessageLogStorage storage = new AolMessageLogStorage(tempDir, true);
            storage.append("q.stats", msg(1, "data1"));
            storage.acknowledge("q.stats", 0L);

            assertThat(storage.getDataForceCount()).isGreaterThanOrEqualTo(1L);
            assertThat(storage.getAckForceCount()).isGreaterThanOrEqualTo(1L);
            assertThat(storage.getForceCount()).isEqualTo(storage.getDataForceCount() + storage.getAckForceCount());

            // Close prints statistics and is idempotent
            storage.close();
            storage.close(); // second close should not throw or fail
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

