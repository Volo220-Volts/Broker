package com.volo.broker.broker.flow;

import com.volo.broker.config.FlowControlProperties;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class QueueFlowController implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(QueueFlowController.class);

    private final ConcurrentHashMap<String, QueueFlowEntry> queues = new ConcurrentHashMap<>();
    private final GlobalMemoryGuard globalMemoryGuard;
    private final int resumeBatch;
    private final long defaultMaxBlockMs;
    private final ExecutorService resumeExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService timeoutScheduler;

    private final AtomicLong totalTransitionsToBlocked = new AtomicLong();
    private final AtomicLong totalTransitionsToOpen = new AtomicLong();
    private final AtomicLong overdueTimeoutRejections = new AtomicLong();

    public QueueFlowController(GlobalMemoryGuard globalMemoryGuard, FlowControlProperties properties) {
        this.globalMemoryGuard = Objects.requireNonNull(globalMemoryGuard, "globalMemoryGuard must not be null");
        this.resumeBatch = properties != null && properties.resumeBatch() > 0 ? properties.resumeBatch() : 64;
        this.defaultMaxBlockMs = properties != null && properties.defaults() != null ? properties.defaults().maxBlockMs() : 0;

        this.timeoutScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "flow-control-timeout-checker");
            t.setDaemon(true);
            return t;
        });
        this.timeoutScheduler.scheduleAtFixedRate(this::checkOverdueGates, 200, 200, TimeUnit.MILLISECONDS);
    }

    public boolean isAnyBlocked(Collection<String> queuePaths) {
        if (globalMemoryGuard.isBlocked()) {
            return true;
        }

        if (queuePaths == null || queuePaths.isEmpty()) {
            return false;
        }

        for (String queuePath : queuePaths) {
            QueueFlowEntry entry = queues.get(queuePath);
            if (entry != null && entry.state == FlowState.BLOCKED) {
                return true;
            }
        }

        return false;
    }

    public boolean isQueueBlocked(String queuePath) {
        if (globalMemoryGuard.isBlocked()) {
            return true;
        }
        QueueFlowEntry entry = queues.get(queuePath);
        return entry != null && entry.state == FlowState.BLOCKED;
    }

    public void park(FlowGate gate, Collection<String> blockedQueuePaths) {
        Objects.requireNonNull(gate, "gate must not be null");
        if (blockedQueuePaths != null && !blockedQueuePaths.isEmpty()) {
            for (String queuePath : blockedQueuePaths) {
                queues.computeIfAbsent(queuePath, k -> new QueueFlowEntry()).waiters.add(gate);
            }
        }

        // Double-check to prevent lost wakeup
        if (!isAnyBlocked(blockedQueuePaths)) {
            unregister(gate);
            gate.resume();
        }
    }

    public void unregister(FlowGate gate) {
        if (gate == null) return;
        for (QueueFlowEntry entry : queues.values()) {
            entry.waiters.remove(gate);
        }
    }

    public void handleTransition(String queuePath, FlowTransition transition) {
        handleTransition(queuePath, transition, defaultMaxBlockMs);
    }

    public void handleTransition(String queuePath, FlowTransition transition, long maxBlockMs) {
        if (transition == null || transition == FlowTransition.NONE) {
            return;
        }

        QueueFlowEntry entry = queues.computeIfAbsent(queuePath, k -> new QueueFlowEntry());
        if (maxBlockMs > 0) {
            entry.maxBlockMs = maxBlockMs;
        }

        if (transition == FlowTransition.TO_BLOCKED) {
            entry.state = FlowState.BLOCKED;
            totalTransitionsToBlocked.incrementAndGet();
            log.debug("Queue '{}' transitioned to BLOCKED (maxBlockMs={})", queuePath, entry.maxBlockMs);
        } else if (transition == FlowTransition.TO_OPEN) {
            entry.state = FlowState.OPEN;
            totalTransitionsToOpen.incrementAndGet();
            log.debug("Queue '{}' transitioned to OPEN. Resuming waiters...", queuePath);
            wakeWaiters(entry);
        }
    }

    public void handleGlobalTransition(FlowTransition transition) {
        if (transition == FlowTransition.TO_OPEN) {
            totalTransitionsToOpen.incrementAndGet();
            log.debug("GlobalMemoryGuard transitioned to OPEN. Resuming all eligible waiters...");
            for (QueueFlowEntry entry : queues.values()) {
                if (entry.state == FlowState.OPEN) {
                    wakeWaiters(entry);
                }
            }
        } else if (transition == FlowTransition.TO_BLOCKED) {
            totalTransitionsToBlocked.incrementAndGet();
            log.debug("GlobalMemoryGuard transitioned to BLOCKED");
        }
    }

    public void onQueueDeleted(String queuePath) {
        QueueFlowEntry removed = queues.remove(queuePath);
        if (removed != null) {
            wakeWaiters(removed);
        }
    }

    public void recordOverdueRejection() {
        overdueTimeoutRejections.incrementAndGet();
    }

    public void checkOverdueGates() {
        long now = System.currentTimeMillis();
        for (QueueFlowEntry entry : queues.values()) {
            if (entry.state == FlowState.BLOCKED) {
                long effectiveTimeout = entry.maxBlockMs > 0 ? entry.maxBlockMs : defaultMaxBlockMs;
                if (effectiveTimeout > 0) {
                    for (FlowGate gate : entry.waiters) {
                        long pausedAt = gate.pausedAtTimestamp();
                        if (pausedAt > 0 && (now - pausedAt) >= effectiveTimeout) {
                            gate.notifyOverdue();
                        }
                    }
                }
            }
        }
    }

    private void wakeWaiters(QueueFlowEntry entry) {
        if (entry.waiters.isEmpty()) {
            return;
        }

        List<FlowGate> waitersToWake = new ArrayList<>(entry.waiters);
        entry.waiters.clear();

        for (int i = 0; i < waitersToWake.size(); i += resumeBatch) {
            int end = Math.min(i + resumeBatch, waitersToWake.size());
            List<FlowGate> batch = new ArrayList<>(waitersToWake.subList(i, end));
            resumeExecutor.submit(() -> {
                for (FlowGate gate : batch) {
                    try {
                        if (!isGateBlockedOnAnyOtherQueue(gate)) {
                            gate.resume();
                        }
                    } catch (Throwable t) {
                        log.warn("Error while resuming flow gate: {}", t.getMessage());
                    }
                }
            });
        }
    }

    private boolean isGateBlockedOnAnyOtherQueue(FlowGate gate) {
        if (globalMemoryGuard.isBlocked()) {
            return true;
        }
        for (QueueFlowEntry entry : queues.values()) {
            if (entry.state == FlowState.BLOCKED && entry.waiters.contains(gate)) {
                return true;
            }
        }
        return false;
    }

    public long totalTransitionsToBlocked() {
        return totalTransitionsToBlocked.get();
    }

    public long totalTransitionsToOpen() {
        return totalTransitionsToOpen.get();
    }

    public long overdueTimeoutRejections() {
        return overdueTimeoutRejections.get();
    }

    @Override
    @PreDestroy
    public void close() {
        timeoutScheduler.shutdownNow();
        resumeExecutor.shutdown();
        log.info("""
                =======================================================
                [Flow Control Shutdown] Statistics:
                  - Total queue transitions to BLOCKED : {}
                  - Total queue transitions to OPEN    : {}
                  - Overdue timeout rejections         : {}
                =======================================================""",
                totalTransitionsToBlocked.get(),
                totalTransitionsToOpen.get(),
                overdueTimeoutRejections.get()
        );
    }

    private static final class QueueFlowEntry {
        volatile FlowState state = FlowState.OPEN;
        volatile long maxBlockMs = 0;
        final Set<FlowGate> waiters = ConcurrentHashMap.newKeySet();
    }
}
