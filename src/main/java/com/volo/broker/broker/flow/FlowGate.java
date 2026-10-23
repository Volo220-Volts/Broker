package com.volo.broker.broker.flow;

public interface FlowGate {
    void resume();
    long pausedAtTimestamp();
    void notifyOverdue();
    void onFlowNotice(boolean blocked, String reason);
}

