package com.volo.broker.broker.command.handler;

import com.volo.broker.broker.command.CommandHandler;
import com.volo.broker.broker.command.model.DeleteQueueCommand;
import com.volo.broker.broker.command.model.QueueOperationResult;
import com.volo.broker.broker.queue.QueueManagementService;

import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class DeleteQueueCommandHandler implements CommandHandler<DeleteQueueCommand, QueueOperationResult> {

    private final QueueManagementService queueManagementService;

    public DeleteQueueCommandHandler(QueueManagementService queueManagementService) {
        this.queueManagementService = Objects.requireNonNull(queueManagementService, "queueManagementService must not be null");
    }

    @Override
    public Class<DeleteQueueCommand> getCommandType() {
        return DeleteQueueCommand.class;
    }

    @Override
    public QueueOperationResult handle(DeleteQueueCommand command) {
        return queueManagementService.deleteQueue(command.queuePath());
    }
}
