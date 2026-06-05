package com.volo.broker.background;


public interface BackgroundTask extends Runnable{
    TaskSchedule schedule();    
}
