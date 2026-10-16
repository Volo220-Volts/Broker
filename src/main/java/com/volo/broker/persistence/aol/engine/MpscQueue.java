package com.volo.broker.persistence.aol.engine;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * High-performance Multi-Producer Single-Consumer (MPSC) lock-free queue based on Dmitry Vyukov's algorithm.
 *
 * <p>Producers perform wait-free enqueues using an atomic {@code getAndSet} on the tail pointer.
 * The single consumer reads from the head pointer without atomic CAS or lock contention.
 *
 * <p>Supports efficient batch draining and park/unpark notification for the single consumer.
 *
 * @param <E> the type of elements held in this queue
 */
public final class MpscQueue<E> {

    private static final class Node<E> {
        E value;
        volatile Node<E> next;

        Node(E value) {
            this.value = value;
        }
    }

    private final AtomicReference<Node<E>> tail;
    private Node<E> head; // Only accessed by the single consumer thread

    private volatile Thread consumerThread;
    private volatile boolean consumerWaiting = false;
    private volatile boolean running = true;

    public MpscQueue() {
        Node<E> stub = new Node<>(null);
        this.tail = new AtomicReference<>(stub);
        this.head = stub;
    }

    public MpscQueue(Thread consumerThread) {
        this();
        this.consumerThread = consumerThread;
    }

    public void setConsumerThread(Thread consumerThread) {
        this.consumerThread = consumerThread;
    }

    /**
     * Enqueues an element in a wait-free manner. Safe for multiple concurrent producers.
     *
     * @param value element to enqueue
     */
    public void offer(E value) {
        Objects.requireNonNull(value, "value");
        Node<E> node = new Node<>(value);
        Node<E> prev = tail.getAndSet(node);
        prev.next = node;

        if (consumerWaiting) {
            Thread t = consumerThread;
            if (t != null) {
                LockSupport.unpark(t);
            }
        }
    }

    /**
     * Polls a single element from the head. Safe only for the single consumer thread.
     *
     * @return the next element, or {@code null} if the queue is empty
     */
    public E poll() {
        Node<E> next = head.next;
        if (next == null) {
            if (head != tail.get()) {
                // Producer called getAndSet but has not linked prev.next yet; brief spin
                while ((next = head.next) == null) {
                    Thread.onSpinWait();
                }
            } else {
                return null;
            }
        }
        head = next;
        E value = next.value;
        next.value = null; // GC friendly
        return value;
    }

    /**
     * Drains all currently available elements up to {@code maxBatch} without blocking.
     *
     * @param action consumer callback
     * @param maxBatch maximum elements to drain
     * @return number of elements drained
     */
    public int drainAvailable(Consumer<? super E> action, int maxBatch) {
        int count = 0;
        while (count < maxBatch) {
            Node<E> next = head.next;
            if (next == null) {
                if (head != tail.get()) {
                    while ((next = head.next) == null) {
                        Thread.onSpinWait();
                    }
                } else {
                    break;
                }
            }
            head = next;
            E value = next.value;
            next.value = null;
            action.accept(value);
            count++;
        }
        return count;
    }

    /**
     * Waits up to {@code timeoutNanos} for elements to become available and drains up to {@code maxBatch}.
     *
     * @param action       consumer callback for drained elements
     * @param maxBatch     maximum number of elements to drain
     * @param timeoutNanos maximum nanoseconds to wait; 0 to wait indefinitely until items arrive or queue stops;
     *                     negative to return immediately without blocking
     * @return number of elements drained (0 if timed out or stopped)
     * @throws InterruptedException if interrupted while waiting
     */
    public int drainBatch(Consumer<? super E> action, int maxBatch, long timeoutNanos) throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        int count = drainAvailable(action, maxBatch);
        if (count > 0 || timeoutNanos < 0 || !running) {
            return count;
        }

        long start = System.nanoTime();
        long remaining = timeoutNanos;

        while (running) {
            consumerWaiting = true;
            try {
                if (head != tail.get() || head.next != null) {
                    break;
                }
                if (timeoutNanos > 0) {
                    LockSupport.parkNanos(this, remaining);
                    long elapsed = System.nanoTime() - start;
                    remaining = timeoutNanos - elapsed;
                    if (remaining <= 0) {
                        break;
                    }
                } else {
                    LockSupport.park(this);
                }
            } finally {
                consumerWaiting = false;
            }

            if (Thread.interrupted()) {
                throw new InterruptedException();
            }

            if (head != tail.get() || head.next != null || !running) {
                break;
            }
        }

        return drainAvailable(action, maxBatch);
    }

    /**
     * Checks if the queue might have pending elements.
     */
    public boolean hasPending() {
        return head.next != null || head != tail.get();
    }

    public boolean isEmpty() {
        return !hasPending();
    }

    /**
     * Stops the queue and wakes up the consumer thread if waiting.
     */
    public void stop() {
        running = false;
        Thread t = consumerThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }
}

