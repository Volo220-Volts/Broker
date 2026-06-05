package com.volo.broker.broker.command.handler;

import org.springframework.stereotype.Component;

import com.volo.broker.broker.command.CommandHandler;
import com.volo.broker.broker.command.model.NackCommand;
import com.volo.broker.broker.services.ConsumerAcknowledgementService;
import lombok.AllArgsConstructor;

@Component
@AllArgsConstructor
public class NackCommandHandler implements CommandHandler<NackCommand, Void> {

    private final ConsumerAcknowledgementService acknowledgementService;
     
    @Override
    public Class<NackCommand> getCommandType() {
        return NackCommand.class;
    }

    @Override
    public Void handle(NackCommand cmd) {
        acknowledgementService.nack(cmd);
        return null;
    }
}
