package com.volo.broker.persistence.aol.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("MpscQueue")
class MpscQueueTest {

    @Test
    void offer_and_poll_single_thread() {
        MpscQueue<String> queue = new MpscQueue<>();
        assertThat(queue.isEmpty()).isTrue();
        assertThat(queue.poll()).isNull();

        queue.offer("A");
        queue.offer("B");
        queue.offer("C");

        assertThat(queue.isEmpty()).isFalse();
        assertThat(queue.poll()).isEqualTo("A");
        assertThat(queue.poll()).isEqualTo("B");
        assertThat(queue.poll()).isEqualTo("C");
        assertThat(queue.poll()).isNull();
        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    void drain_available_extracts_up_to_max_batch() {
        MpscQueue<Integer> queue = new MpscQueue<>();
        for (int i = 0; i < 10; i++) {
            queue.offer(i);
        }

        List<Integer> batch1 = new ArrayList<>();
        int c1 = queue.drainAvailable(batch1::add, 4);
        assertThat(c1).isEqualTo(4);
        assertThat(batch1).containsExactly(0, 1, 2, 3);

        List<Integer> batch2 = new ArrayList<>();
        int c2 = queue.drainAvailable(batch2::add, 10);
        assertThat(c2).isEqualTo(6);
        assertThat(batch2).containsExactly(4, 5, 6, 7, 8, 9);

        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    void multi_producer_concurrent_enqueues_with_single_consumer() throws Exception {
        MpscQueue<Long> queue = new MpscQueue<>();
        int producerCount = 8;
        int itemsPerProducer = 10_000;
        int totalItems = producerCount * itemsPerProducer;

        ExecutorService producerPool = Executors.newFixedThreadPool(producerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(producerCount);

        for (int p = 0; p < producerCount; p++) {
            final long base = (long) p * 1_000_000L;
            producerPool.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < itemsPerProducer; i++) {
                        queue.offer(base + i);
                    }
                } catch (InterruptedException ignored) {
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        // Single consumer thread draining from queue
        List<Long> received = new ArrayList<>(totalItems);
        queue.setConsumerThread(Thread.currentThread());

        startLatch.countDown();

        while (received.size() < totalItems) {
            queue.drainAvailable(received::add, 256);
            if (received.size() < totalItems) {
                Thread.onSpinWait();
            }
        }

        finishLatch.await(5, TimeUnit.SECONDS);
        producerPool.shutdown();

        assertThat(received).hasSize(totalItems);
        Set<Long> unique = new HashSet<>(received);
        assertThat(unique).hasSize(totalItems);
    }

    @Test
    void drain_batch_waits_for_producer_and_wakes_up() throws Exception {
        MpscQueue<String> queue = new MpscQueue<>();
        CountDownLatch readyLatch = new CountDownLatch(1);
        List<String> collected = new CopyOnWriteArrayList<>();

        Thread consumerThread = new Thread(() -> {
            queue.setConsumerThread(Thread.currentThread());
            readyLatch.countDown();
            try {
                int drained = queue.drainBatch(collected::add, 10, 5_000_000_000L); // up to 5s
                assertThat(drained).isGreaterThan(0);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        consumerThread.start();

        readyLatch.await();
        Thread.sleep(50); // Give consumer time to park

        queue.offer("woken_item");
        consumerThread.join(2000);

        assertThat(consumerThread.isAlive()).isFalse();
        assertThat(collected).containsExactly("woken_item");
    }

    @Test
    void stop_unparks_consumer() throws Exception {
        MpscQueue<String> queue = new MpscQueue<>();
        CountDownLatch readyLatch = new CountDownLatch(1);
        AtomicBoolean finished = new AtomicBoolean(false);

        Thread consumerThread = new Thread(() -> {
            queue.setConsumerThread(Thread.currentThread());
            readyLatch.countDown();
            try {
                queue.drainBatch(item -> {}, 10, 0L); // indefinite wait
            } catch (InterruptedException ignored) {
            }
            finished.set(true);
        });
        consumerThread.start();

        readyLatch.await();
        Thread.sleep(50);

        queue.stop();
        consumerThread.join(1000);

        assertThat(finished.get()).isTrue();
    }
}

