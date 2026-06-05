package com.volo.broker.broker.command.handler;

import org.springframework.stereotype.Component;

import com.volo.broker.broker.command.CommandHandler;
import com.volo.broker.broker.command.model.AckCommand;
import com.volo.broker.broker.services.ConsumerAcknowledgementService;

import lombok.AllArgsConstructor;

@Component
@AllArgsConstructor

public class AckCommandHandler implements CommandHandler<AckCommand, Void> {

    private final ConsumerAcknowledgementService acknowledgementService;


    @Override
    public Class<AckCommand> getCommandType() {
        return AckCommand.class;
    }

    @Override
    public Void handle(AckCommand command) {
        acknowledgementService.ack(command);
        return null;
    }
}
