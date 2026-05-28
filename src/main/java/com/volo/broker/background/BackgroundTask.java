package com.volo.broker.background;


public interface BackgroundTask {
    
    String name();

    
    void execute();
}
