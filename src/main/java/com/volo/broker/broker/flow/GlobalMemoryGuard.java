package com.volo.broker.broker.flow;

import com.volo.broker.config.FlowControlProperties;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class GlobalMemoryGuard {
    private final AtomicLong totalReadyBytes = new AtomicLong(0);
    private final long highBytes;
    private final long lowBytes;
    private final boolean enabled;
    private volatile FlowState globalState = FlowState.OPEN;

    public GlobalMemoryGuard(FlowControlProperties properties) {
        if (properties != null && properties.enabled() && properties.global() != null) {
            this.enabled = true;
            this.highBytes = properties.global().highBytes();
            this.lowBytes = properties.global().lowBytes();
        } else {
            this.enabled = false;
            this.highBytes = Long.MAX_VALUE;
            this.lowBytes = Long.MAX_VALUE / 2;
        }
    }

    public FlowTransition recordBytesAdded(long bytes) {
        if (bytes <= 0 || !enabled) {
            return FlowTransition.NONE;
        }

        long current = totalReadyBytes.addAndGet(bytes);
        if (globalState == FlowState.OPEN && current >= highBytes) {
            synchronized (this) {
                if (globalState == FlowState.OPEN && totalReadyBytes.get() >= highBytes) {
                    globalState = FlowState.BLOCKED;
                    return FlowTransition.TO_BLOCKED;
                }
            }
        }
        return FlowTransition.NONE;
    }

    public FlowTransition recordBytesRemoved(long bytes) {
        if (bytes <= 0 || !enabled) {
            return FlowTransition.NONE;
        }

        long current = totalReadyBytes.addAndGet(-bytes);
        if (current < 0) {
            totalReadyBytes.compareAndSet(current, 0);
            current = 0;
        }

        if (globalState == FlowState.BLOCKED && current <= lowBytes) {
            synchronized (this) {
                if (globalState == FlowState.BLOCKED && totalReadyBytes.get() <= lowBytes) {
                    globalState = FlowState.OPEN;
                    return FlowTransition.TO_OPEN;
                }
            }
        }
        return FlowTransition.NONE;
    }

    public boolean isBlocked() {
        return enabled && globalState == FlowState.BLOCKED;
    }

    public FlowState globalState() {
        return globalState;
    }

    public long totalReadyBytes() {
        return totalReadyBytes.get();
    }
}

