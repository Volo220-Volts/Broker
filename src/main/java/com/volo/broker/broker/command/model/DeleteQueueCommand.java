package com.volo.broker.broker.command.model;

import com.volo.broker.broker.command.TransportCommand;

public record DeleteQueueCommand(String queuePath) implements TransportCommand {
}
