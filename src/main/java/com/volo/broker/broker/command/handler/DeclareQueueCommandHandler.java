package com.volo.broker.broker.command.handler;

import com.volo.broker.broker.command.CommandHandler;
import com.volo.broker.broker.command.model.DeclareQueueCommand;
import com.volo.broker.broker.command.model.QueueOperationResult;
import com.volo.broker.broker.queue.QueueManagementService;

import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class DeclareQueueCommandHandler implements CommandHandler<DeclareQueueCommand, QueueOperationResult> {

    private final QueueManagementService queueManagementService;

    public DeclareQueueCommandHandler(QueueManagementService queueManagementService) {
        this.queueManagementService = Objects.requireNonNull(queueManagementService, "queueManagementService must not be null");
    }

    @Override
    public Class<DeclareQueueCommand> getCommandType() {
        return DeclareQueueCommand.class;
    }

    @Override
    public QueueOperationResult handle(DeclareQueueCommand command) {
        return queueManagementService.declareQueue(command.queuePath());
    }
}
