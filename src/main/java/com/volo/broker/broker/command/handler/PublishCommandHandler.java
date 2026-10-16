package com.volo.broker.broker.command.handler;

import com.volo.broker.broker.command.CommandHandler;
import com.volo.broker.broker.command.model.PublishCommand;
import com.volo.broker.broker.command.model.PublishResult;
import com.volo.broker.broker.services.PublishService;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class PublishCommandHandler implements CommandHandler<PublishCommand, PublishResult> {
    private final PublishService publishService;

    public PublishCommandHandler(PublishService publishService) 
    {
        this.publishService = Objects.requireNonNull(publishService, "publishService must not be null");
    }   
    

    @Override
    public Class<PublishCommand> getCommandType() {
        return PublishCommand.class;
    }

    @Override
    public PublishResult handle(PublishCommand command) {
        return publishService.publish(command);
    }
}
