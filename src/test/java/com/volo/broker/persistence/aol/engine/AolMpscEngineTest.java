package com.volo.broker.persistence.aol.engine;

import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.persistence.aol.api.WriteResult;
import com.volo.broker.persistence.aol.codec.DataRecordCodec;
import com.volo.broker.persistence.aol.io.AolFile;
import com.volo.broker.persistence.aol.io.AolFileChannel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(20)
@DisplayName("AolMpscEngine")
class AolMpscEngineTest {

    @TempDir
    Path tempDir;

    private final DataRecordCodec codec = new DataRecordCodec();

    @Test
    @DisplayName("Sequential fsync commits messages monotonically")
    void sequential_fsync_commits_messages_monotonically() throws Exception {
        Path dataPath = tempDir.resolve("seq_mpsc_data.log");
        Path ackedPath = tempDir.resolve("seq_mpsc_acked.bin");

        AolFile dataFile = AolFileChannel.createNew(dataPath);
        AolFile ackedFile = AolFileChannel.createNew(ackedPath);

        try (AolMpscEngine engine = new AolMpscEngine(codec, FsyncMode.SEQUENTIAL)) {
            engine.registerQueue("orders", dataFile, ackedFile, 0L);

            WriteResult r1 = engine.append("orders", msg(1, "hello")).join();
            WriteResult r2 = engine.append("orders", msg(2, "world")).join();

            assertThat(r1.queueOffset()).isZero();
            assertThat(r2.queueOffset()).isEqualTo(1L);
            assertThat(r1.physicalPosition()).isLessThan(r2.physicalPosition());
            assertThat(engine.getFsyncMode()).isEqualTo(FsyncMode.SEQUENTIAL);
        }

        dataFile.close();
        ackedFile.close();
    }

    @Test
    @DisplayName("Parallel fsync mode commits across multiple queues")
    void parallel_fsync_commits_across_multiple_queues() throws Exception {
        Path q1Data = tempDir.resolve("q1_data.log");
        Path q1Ack = tempDir.resolve("q1_acked.bin");
        Path q2Data = tempDir.resolve("q2_data.log");
        Path q2Ack = tempDir.resolve("q2_acked.bin");

        AolFile f1 = AolFileChannel.createNew(q1Data);
        AolFile a1 = AolFileChannel.createNew(q1Ack);
        AolFile f2 = AolFileChannel.createNew(q2Data);
        AolFile a2 = AolFileChannel.createNew(q2Ack);

        try (AolMpscEngine engine = new AolMpscEngine(codec, FsyncMode.PARALLEL)) {
            engine.registerQueue("q1", f1, a1, 0L);
            engine.registerQueue("q2", f2, a2, 0L);

            CompletableFuture<WriteResult> fut1 = engine.append("q1", msg(10, "m1"));
            CompletableFuture<WriteResult> fut2 = engine.append("q2", msg(20, "m2"));

            WriteResult res1 = fut1.join();
            WriteResult res2 = fut2.join();

            assertThat(res1.queueOffset()).isZero();
            assertThat(res2.queueOffset()).isZero();
            assertThat(engine.getFsyncMode()).isEqualTo(FsyncMode.PARALLEL);
        }

        f1.close(); a1.close();
        f2.close(); a2.close();
    }

    @Test
    @DisplayName("Switching fsync mode dynamically between SEQUENTIAL and PARALLEL works seamlessly")
    void switching_fsync_mode_dynamically() throws Exception {
        Path dataPath = tempDir.resolve("switch_mpsc_data.log");
        Path ackedPath = tempDir.resolve("switch_mpsc_acked.bin");

        AolFile dataFile = AolFileChannel.createNew(dataPath);
        AolFile ackedFile = AolFileChannel.createNew(ackedPath);

        try (AolMpscEngine engine = new AolMpscEngine(codec, FsyncMode.SEQUENTIAL)) {
            engine.registerQueue("test.q", dataFile, ackedFile, 0L);

            WriteResult r0 = engine.append("test.q", msg(1, "part1")).join();
            assertThat(r0.queueOffset()).isZero();
            assertThat(engine.getFsyncMode()).isEqualTo(FsyncMode.SEQUENTIAL);

            // Switch to PARALLEL
            engine.setFsyncMode(FsyncMode.PARALLEL);
            assertThat(engine.getFsyncMode()).isEqualTo(FsyncMode.PARALLEL);

            WriteResult r1 = engine.append("test.q", msg(2, "part2")).join();
            assertThat(r1.queueOffset()).isEqualTo(1L);

            // Switch back to SEQUENTIAL
            engine.setFsyncMode(FsyncMode.SEQUENTIAL);
            assertThat(engine.getFsyncMode()).isEqualTo(FsyncMode.SEQUENTIAL);

            WriteResult r2 = engine.append("test.q", msg(3, "part3")).join();
            assertThat(r2.queueOffset()).isEqualTo(2L);
        }

        dataFile.close();
        ackedFile.close();
    }

    @Test
    @DisplayName("Concurrent producers across multiple threads batch naturally without contention")
    void concurrent_producers_batch_naturally() throws Exception {
        int threads = 32;
        int msgsPerThread = 50;

        Path qData = tempDir.resolve("concurrent_mpsc_data.log");
        Path qAck = tempDir.resolve("concurrent_mpsc_acked.bin");
        AolFile dataFile = AolFileChannel.createNew(qData);
        AolFile ackedFile = AolFileChannel.createNew(qAck);

        try (AolMpscEngine engine = new AolMpscEngine(codec, FsyncMode.PARALLEL)) {
            engine.registerQueue("bench.q", dataFile, ackedFile, 0L);

            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();

            Set<Long> writtenOffsets = ConcurrentHashMap.newKeySet();

            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int m = 0; m < msgsPerThread; m++) {
                        long msgId = (long) threadId * 10_000 + m;
                        WriteResult res = engine.append("bench.q", msg(msgId, "payload-" + msgId)).join();
                        writtenOffsets.add(res.queueOffset());
                    }
                    return null;
                }));
            }

            start.countDown();
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
            pool.shutdown();

            assertThat(writtenOffsets).hasSize(threads * msgsPerThread);
            assertThat(writtenOffsets).contains(0L, (long) (threads * msgsPerThread - 1));
        }

        dataFile.close();
        ackedFile.close();
    }

    @Test
    @DisplayName("Fsync failure propagates to pending futures and enters failed state")
    void fsync_failure_propagates_to_pending_futures() throws Exception {
        FakeAolFile fakeData = new FakeAolFile(0);
        FakeAolFile fakeAck = new FakeAolFile(0);
        fakeData.failForce = true;

        try {
            AolMpscEngine engine = new AolMpscEngine(codec, FsyncMode.SEQUENTIAL);
            engine.registerQueue("failing.q", fakeData, fakeAck, 0L);

            CompletableFuture<WriteResult> fut = engine.append("failing.q", msg(1, "fail"));

            assertThatThrownBy(fut::join)
                    .hasCauseInstanceOf(IOException.class)
                    .hasMessageContaining("simulated disk error");

            // Subsequent appends should fail immediately
            assertThatThrownBy(() -> engine.append("failing.q", msg(2, "after_fail")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("FAILED");

            assertThatThrownBy(engine::close)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("simulated disk error");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IOException.class);
        }
    }

    @Test
    @DisplayName("Batching drastically reduces force count under concurrent load")
    void batching_drastically_reduces_force_count() throws Exception {
        int threads = 16;
        int msgsPerThread = 50;

        Path qData = tempDir.resolve("batched_mpsc_data.log");
        Path qAck = tempDir.resolve("batched_mpsc_acked.bin");
        AolFile dataFile = AolFileChannel.createNew(qData);
        AolFile ackedFile = AolFileChannel.createNew(qAck);

        try (AolMpscEngine engine = new AolMpscEngine(codec, FsyncMode.PARALLEL)) {
            engine.setBatchConfig(64, 2000L); // 64 batch size, 2ms linger
            engine.registerQueue("batched.q", dataFile, ackedFile, 0L);

            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch latch = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < threads; t++) {
                final int tid = t;
                futures.add(pool.submit(() -> {
                    latch.await();
                    for (int m = 0; m < msgsPerThread; m++) {
                        engine.append("batched.q", msg(tid * 1000L + m, "body")).join();
                    }
                    return null;
                }));
            }

            latch.countDown();
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
            pool.shutdown();

            long forceCount = engine.getForceCount();
            assertThat(forceCount)
                    .isGreaterThan(0)
                    .isLessThan(60);
        }

        dataFile.close();
        ackedFile.close();
    }

    @Test
    @DisplayName("ACKs piggyback on data writes without independent force calls")
    void acks_piggyback_on_data_writes() throws Exception {
        Path qData = tempDir.resolve("piggy_mpsc_data.log");
        Path qAck = tempDir.resolve("piggy_mpsc_acked.bin");
        AolFile dataFile = AolFileChannel.createNew(qData);
        AolFile ackedFile = AolFileChannel.createNew(qAck);

        try (AolMpscEngine engine = new AolMpscEngine(codec, FsyncMode.PARALLEL)) {
            engine.setBatchConfig(64, 2000L);
            engine.registerQueue("piggy.q", dataFile, ackedFile, 0L);

            List<CompletableFuture<?>> all = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                all.add(engine.append("piggy.q", msg(i, "data-" + i)));
                all.add(engine.acknowledge("piggy.q", i));
            }

            CompletableFuture.allOf(all.toArray(CompletableFuture[]::new)).join();

            assertThat(engine.getForceCount())
                    .isGreaterThan(0)
                    .isLessThan(20);
            assertThat(engine.getDataForceCount()).isGreaterThan(0);
            assertThat(engine.getAckForceCount()).isGreaterThan(0);
            assertThat(engine.getForceCount()).isEqualTo(engine.getDataForceCount() + engine.getAckForceCount());
        }

        dataFile.close();
        ackedFile.close();
    }

    // ------------------------------------------------------------------ helpers

    private static QueuedMessage msg(long id, String payload) {
        return new QueuedMessage(id, payload.getBytes(), Map.of(), "key",
                1_700_000_000_000L, false, 0);
    }

    private static final class FakeAolFile implements AolFile {
        final AtomicInteger forceCount = new AtomicInteger();
        final AtomicLong size;
        volatile boolean failForce = false;

        FakeAolFile(long initialSize) {
            this.size = new AtomicLong(initialSize);
        }

        @Override
        public void force() throws IOException {
            forceCount.incrementAndGet();
            if (failForce) {
                throw new IOException("simulated disk error");
            }
        }

        @Override public long write(ByteBuffer data) { return size.getAndAdd(data.remaining()); }
        @Override public ByteBuffer read(long position, int length) { return ByteBuffer.allocate(length); }
        @Override public long size() { return size.get(); }
        @Override public void truncate(long newSize) { size.set(newSize); }
        @Override public Path path() { return Path.of("fake.log"); }
        @Override public void close() {}
    }
}

