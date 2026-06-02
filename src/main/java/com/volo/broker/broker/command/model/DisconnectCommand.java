package com.volo.broker.broker.command.model;

import com.volo.broker.broker.command.TransportCommand;

public record DisconnectCommand(String consumerTag) implements TransportCommand {

    public DisconnectCommand(String consumerTag) {
        this.consumerTag = consumerTag;
    }

}
