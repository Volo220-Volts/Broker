package com.volo.broker.broker.services;

import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.queue.settings.QueueSettings;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.validation.QueuePathValidator;
import com.volo.broker.broker.flow.QueueFlowController;
import org.springframework.beans.factory.annotation.Autowired;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class QueueDeclarationService {
    private final QueueManager queueManager;
    private final QueuePathValidator queuePathValidator;
    private final QueuePersistenceService persistenceService;
    private final QueueFlowController flowController;

    public QueueDeclarationService(
            QueueManager queueManager,
            QueuePathValidator queuePathValidator,
            QueuePersistenceService persistenceService
    ) {
        this(queueManager, queuePathValidator, persistenceService, null);
    }

    @Autowired
    public QueueDeclarationService(
            QueueManager queueManager,
            QueuePathValidator queuePathValidator,
            QueuePersistenceService persistenceService,
            @Nullable QueueFlowController flowController
    ) {
        this.queueManager = Objects.requireNonNull(queueManager);
        this.queuePathValidator = Objects.requireNonNull(queuePathValidator);
        this.persistenceService = Objects.requireNonNull(persistenceService);
        this.flowController = flowController;
    }

    public BrokerQueue declare(String rawQueuePath, QueueSettings settings) {
        QueuePath queuePath = queuePathValidator.validateAndParse(rawQueuePath);
        QueueSettings effectiveSettings = settings == null
                ? QueueSettings.defaults()
                : settings;

        BrokerQueue queue = queueManager.declare(queuePath, effectiveSettings);

        try {
            persistenceService.saveQueueIfDurable(queue);
            return queue;
        } catch (Throwable throwable) {
            queueManager.delete(queuePath);
            throw throwable;
        }
    }

    public void delete(String rawQueuePath) {
        QueuePath queuePath = queuePathValidator.validateAndParse(rawQueuePath);

        queueManager.require(queuePath.value());

        persistenceService.deleteQueue(queuePath.value());
        queueManager.delete(queuePath);
        if (flowController != null) {
            flowController.onQueueDeleted(queuePath.value());
        }
    }
}