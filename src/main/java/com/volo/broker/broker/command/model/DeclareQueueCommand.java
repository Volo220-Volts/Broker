package com.volo.broker.broker.command.model;

import com.volo.broker.broker.command.TransportCommand;

public record DeclareQueueCommand(String queuePath) implements TransportCommand {
}
