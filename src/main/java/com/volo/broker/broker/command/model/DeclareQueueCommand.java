package com.volo.broker.broker.command.model;

import com.volo.broker.broker.command.TransportCommand;
import com.volo.broker.broker.queue.settings.QueueSettings;

public record DeclareQueueCommand(String queuePath, QueueSettings settings) implements TransportCommand {
}
