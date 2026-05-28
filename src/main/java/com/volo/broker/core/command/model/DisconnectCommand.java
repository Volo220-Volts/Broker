package com.volo.broker.core.command.model;

import com.volo.broker.core.command.TransportCommand;

public class DisconnectCommand implements TransportCommand {

    public final String consumerTag;

    public DisconnectCommand(String consumerTag) {
        this.consumerTag = consumerTag;
    }

}
