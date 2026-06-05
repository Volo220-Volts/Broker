package com.volo.broker.broker.command.handler;

import com.volo.broker.broker.command.CommandHandler;
import com.volo.broker.broker.command.model.DeleteQueueCommand;
import com.volo.broker.broker.command.model.QueueOperationResult;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.queue.exception.QueueNotFoundException;
import com.volo.broker.broker.services.QueueDeclarationService;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class DeleteQueueCommandHandler implements CommandHandler<DeleteQueueCommand, QueueOperationResult> {

    private final QueueDeclarationService declarationService;

    public DeleteQueueCommandHandler(QueueDeclarationService declarationService) {
        this.declarationService = Objects.requireNonNull(declarationService);
    }

    @Override
    public Class<DeleteQueueCommand> getCommandType() {
        return DeleteQueueCommand.class;
    }

    @Override
    public QueueOperationResult handle(DeleteQueueCommand command) {
        try {
            declarationService.delete(command.queuePath());
            return QueueOperationResult.ofSuccess();

        } catch (QueueNotFoundException exception) {
            return QueueOperationResult.ofFailure(
                    BrokerErrorCode.QUEUE_NOT_FOUND,
                    exception.getMessage()
            );
        } catch (IllegalArgumentException exception) {
            return QueueOperationResult.ofFailure(
                    BrokerErrorCode.INVALID_REQUEST,
                    exception.getMessage()
            );
        } catch (Throwable throwable) {
            return QueueOperationResult.ofFailure(
                    BrokerErrorCode.INTERNAL_ERROR,
                    "Queue was not deleted"
            );
        }
    }
}
