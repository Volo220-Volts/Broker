package com.volo.broker.broker.queue.exception;

public class QueueAlreadyExistsException extends RuntimeException {
    public QueueAlreadyExistsException(String message) {
        super(message);
    }
}