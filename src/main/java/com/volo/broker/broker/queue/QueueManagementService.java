package com.volo.broker.broker.queue;

import com.volo.broker.broker.command.model.QueueOperationResult;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.router.RouteTree;
import com.volo.broker.broker.validation.QueuePathValidator;

import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class QueueManagementService {

    private final QueueManager queueManager;
    private final RouteTree routeTree;
    private final QueuePathValidator queuePathValidator;

    public QueueManagementService(
            QueueManager queueManager,
            RouteTree routeTree,
            QueuePathValidator queuePathValidator
    ) {
        this.queueManager = Objects.requireNonNull(queueManager, "queueManager must not be null");
        this.routeTree = Objects.requireNonNull(routeTree, "routeTree must not be null");
        this.queuePathValidator = Objects.requireNonNull(queuePathValidator, "queuePathValidator must not be null");
    }

    public QueueOperationResult declareQueue(String rawQueuePath) {
        try {
            QueuePath queuePath = queuePathValidator.validateAndParse(rawQueuePath);

            if (queueManager.getQueue(queuePath.value()).isEmpty()) {
                queueManager.createQueue(queuePath.value());
            }

            routeTree.attachQueue(queuePath);

            return QueueOperationResult.ofSuccess();

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

    public QueueOperationResult deleteQueue(String rawQueuePath) {
        try {
            QueuePath queuePath = queuePathValidator.validateAndParse(rawQueuePath);

            routeTree.detachQueue(queuePath);
            queueManager.deleteQueue(queuePath.value());

            return QueueOperationResult.ofSuccess();

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
