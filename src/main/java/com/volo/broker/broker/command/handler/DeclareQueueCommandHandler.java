package com.volo.broker.broker.command.handler;

import com.volo.broker.broker.command.CommandHandler;
import com.volo.broker.broker.command.model.DeclareQueueCommand;
import com.volo.broker.broker.command.model.QueueOperationResult;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.queue.exception.QueueAlreadyExistsException;
import com.volo.broker.broker.queue.settings.QueueSettings;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.validation.QueuePathValidator;

import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class DeclareQueueCommandHandler implements CommandHandler<DeclareQueueCommand, QueueOperationResult> {

    private final QueueManager queueManager;
    private final QueuePathValidator queuePathValidator;

    public DeclareQueueCommandHandler(QueueManager queueManager, QueuePathValidator queuePathValidator) {
        this.queueManager = Objects.requireNonNull(queueManager, "queueManager must not be null");
        this.queuePathValidator = Objects.requireNonNull(queuePathValidator, "queuePathValidator must not be null");
    }

    @Override
    public Class<DeclareQueueCommand> getCommandType() {
        return DeclareQueueCommand.class;
    }

    @Override
    public QueueOperationResult handle(DeclareQueueCommand command) {
        try {
            QueuePath path = queuePathValidator.validateAndParse(command.queuePath());
            QueueSettings settings = command.settings() == null
                    ? QueueSettings.defaults()
                    : command.settings();

            queueManager.declare(path, settings);

            return QueueOperationResult.ofSuccess();

        } catch (QueueAlreadyExistsException exception) {
            return QueueOperationResult.ofFailure(
                    BrokerErrorCode.QUEUE_ALREADY_EXISTS,
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
                    "Queue was not declared"
            );
        }
    }
}
