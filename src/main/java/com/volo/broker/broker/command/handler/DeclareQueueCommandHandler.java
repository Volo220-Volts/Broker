package com.volo.broker.broker.command.handler;

import com.volo.broker.broker.command.CommandHandler;
import com.volo.broker.broker.command.model.DeclareQueueCommand;
import com.volo.broker.broker.command.model.QueueOperationResult;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.queue.exception.QueueAlreadyExistsException;
import com.volo.broker.broker.services.QueueDeclarationService;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class DeclareQueueCommandHandler implements CommandHandler<DeclareQueueCommand, QueueOperationResult> {

    private final QueueDeclarationService declarationService;
    public DeclareQueueCommandHandler(QueueDeclarationService declarationService) {
        this.declarationService = Objects.requireNonNull(declarationService);
    }

    @Override
    public Class<DeclareQueueCommand> getCommandType() {
        return DeclareQueueCommand.class;
    }

    @Override
    public QueueOperationResult handle(DeclareQueueCommand command) {
        try {
            declarationService.declare(command.queuePath(), command.settings());
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
