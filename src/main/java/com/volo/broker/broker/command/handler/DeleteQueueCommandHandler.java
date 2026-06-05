package com.volo.broker.broker.command.handler;

import com.volo.broker.broker.command.CommandHandler;
import com.volo.broker.broker.command.model.DeleteQueueCommand;
import com.volo.broker.broker.command.model.QueueOperationResult;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.queue.exception.QueueNotFoundException;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.validation.QueuePathValidator;

import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class DeleteQueueCommandHandler implements CommandHandler<DeleteQueueCommand, QueueOperationResult> {

    private final QueueManager queueManager;
    private final QueuePathValidator queuePathValidator;

    public DeleteQueueCommandHandler(QueueManager queueManager, QueuePathValidator queuePathValidator) {
        this.queueManager = Objects.requireNonNull(queueManager, "queueManager must not be null");
        this.queuePathValidator = Objects.requireNonNull(queuePathValidator, "queuePathValidator must not be null");
    }

    @Override
    public Class<DeleteQueueCommand> getCommandType() {
        return DeleteQueueCommand.class;
    }

    @Override
    public QueueOperationResult handle(DeleteQueueCommand command) {
        try {
            QueuePath path = queuePathValidator.validateAndParse(command.queuePath());

            queueManager.delete(path);

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
