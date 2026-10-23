package com.volo.broker.broker.flow;

import com.custombroker.grpc.*;
import com.google.protobuf.ByteString;
import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.command.CommandBus;
import com.volo.broker.broker.command.handler.PublishCommandHandler;
import com.volo.broker.broker.dispatch.DispatchManager;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.message.GlobalLongMessageIdGenerator;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.queue.settings.FlowControlPolicy;
import com.volo.broker.broker.services.*;
import com.volo.broker.broker.subscription.DefaultSubscriptionManager;
import com.volo.broker.broker.subscription.SubscriptionManager;
import com.volo.broker.broker.validation.QueuePathValidator;
import com.volo.broker.config.FlowControlProperties;
import com.volo.broker.persistence.MessageStore;
import com.volo.broker.transport.grpc.connection.PublisherConnection;
import com.volo.broker.transport.grpc.connection.PublisherConnectionRegistry;
import com.volo.broker.transport.grpc.service.GrpcPublisherService;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class FlowControlIntegrationTest {

    private QueueManager queueManager;
    private SubscriptionManager subscriptionManager;
    private MessageStore messageStore;
    private QueueDispatchScheduler scheduler;
    private GlobalMemoryGuard memoryGuard;
    private QueueFlowController flowController;
    private MessageLifecycleService lifecycleService;
    private PublishService publishService;
    private DispatchManager dispatchManager;
    private QueueDeclarationService declarationService;
    private CommandBus commandBus;
    private PublisherConnectionRegistry publisherRegistry;
    private GrpcPublisherService grpcPublisherService;

    @BeforeEach
    void setUp() {
        queueManager = new InMemoryQueueManager();
        subscriptionManager = new DefaultSubscriptionManager();
        messageStore = mock(MessageStore.class);
        scheduler = mock(QueueDispatchScheduler.class);

        FlowControlProperties props = new FlowControlProperties(true, null, null, 16);
        memoryGuard = new GlobalMemoryGuard(props);
        flowController = new QueueFlowController(memoryGuard, props);

        lifecycleService = new MessageLifecycleService(messageStore);

        GlobalLongMessageIdGenerator idGenerator = new GlobalLongMessageIdGenerator();
        publishService = new PublishService(
                queueManager,
                messageStore,
                scheduler,
                idGenerator,
                lifecycleService,
                flowController,
                memoryGuard
        );

        dispatchManager = new DispatchManager(
                queueManager,
                subscriptionManager,
                lifecycleService,
                flowController,
                memoryGuard
        );

        new ConsumerAcknowledgementService(
                subscriptionManager,
                queueManager,
                scheduler,
                lifecycleService,
                flowController
        );

        QueuePersistenceService persistenceService = mock(QueuePersistenceService.class);
        declarationService = new QueueDeclarationService(
                queueManager,
                new QueuePathValidator(),
                persistenceService,
                flowController
        );

        commandBus = new CommandBus(List.of(new PublishCommandHandler(publishService)));

        publisherRegistry = new PublisherConnectionRegistry();
        grpcPublisherService = new GrpcPublisherService(commandBus, publisherRegistry, flowController);
    }

    @Test
    @DisplayName("Single producer hits HWM -> pauses stream; consumer polls down to LWM -> unpauses stream")
    @SuppressWarnings("unchecked")
    void singleProducerHwmLwmIntegration() {
        // HWM = 3, LWM = 1
        FlowControlPolicy policy = new FlowControlPolicy(true, 3, 1, 0, 0, 0);
        declarationService.declare("orders.created", TestQueueSettings.withFlowControl(policy));

        ServerCallStreamObserver<PublisherResponse> serverCall = mock(ServerCallStreamObserver.class);
        List<PublisherResponse> responses = new CopyOnWriteArrayList<>();
        doAnswer(inv -> {
            responses.add(inv.getArgument(0));
            return null;
        }).when(serverCall).onNext(any());

        StreamObserver<PublisherRequest> stream = grpcPublisherService.openPublisherChannel(serverCall);
        verify(serverCall, times(1)).disableAutoRequest();
        verify(serverCall, times(1)).request(1);

        PublisherConnection connection = publisherRegistry.activeConnections().iterator().next();
        assertThat(connection.isPaused()).isFalse();

        // Send 1st and 2nd messages -> accepted, gate remains FLOWING, requests next frame
        for (int i = 1; i <= 2; i++) {
            stream.onNext(publishRequest(i, "orders.created"));
            assertThat(connection.isPaused()).isFalse();
        }

        // 3rd message hits HWM (3) -> queue transitions to BLOCKED, gate transitions to PAUSED
        stream.onNext(publishRequest(3, "orders.created"));
        assertThat(connection.isPaused()).isTrue();
        assertThat(connection.flowGate().state()).isEqualTo(com.volo.broker.transport.grpc.flow.PublisherFlowGate.GateState.PAUSED);

        // Verify FlowControlNotice(blocked=true) was sent
        assertThat(responses).anyMatch(r -> r.hasFlow() && r.getFlow().getBlocked());

        // Consumer polls messages directly
        BrokerQueue queue = queueManager.require("orders.created");
        assertThat(queue.size()).isEqualTo(3);

        // Poll 1 message -> size=2 (still > LWM 1) -> gate remains PAUSED
        dispatchManager.dispatchLoop("orders.created"); // no subscriber, so poll manually
        var p1 = queue.poll();
        flowController.handleTransition("orders.created", p1.transition());
        assertThat(queue.size()).isEqualTo(2);
        assertThat(connection.isPaused()).isTrue();

        // Poll 2nd message -> size=1 (<= LWM 1) -> queue TO_OPEN, gate resumed!
        var p2 = queue.poll();
        flowController.handleTransition("orders.created", p2.transition());
        assertThat(queue.size()).isEqualTo(1);

        // Give virtual thread a moment to execute batch resume and deliver flow notice
        awaitCondition(() -> !connection.isPaused() && responses.stream().anyMatch(r -> r.hasFlow() && !r.getFlow().getBlocked()));
        assertThat(connection.isPaused()).isFalse();
        assertThat(connection.flowGate().state()).isEqualTo(com.volo.broker.transport.grpc.flow.PublisherFlowGate.GateState.FLOWING);
        assertThat(responses).anyMatch(r -> r.hasFlow() && !r.getFlow().getBlocked());
    }

    @Test
    @DisplayName("Publishing to wildcard where one queue is full pauses publisher stream")
    @SuppressWarnings("unchecked")
    void multiQueueWildcardFlowTest() {
        // orders.fast has HWM=10, orders.slow has HWM=2
        declarationService.declare("orders.fast", TestQueueSettings.withFlowControl(new FlowControlPolicy(true, 10, 5, 0, 0, 0)));
        declarationService.declare("orders.slow", TestQueueSettings.withFlowControl(new FlowControlPolicy(true, 2, 1, 0, 0, 0)));

        ServerCallStreamObserver<PublisherResponse> serverCall = mock(ServerCallStreamObserver.class);
        StreamObserver<PublisherRequest> stream = grpcPublisherService.openPublisherChannel(serverCall);

        PublisherConnection connection = publisherRegistry.activeConnections().iterator().next();

        // Publish to "orders.*" -> routes to both
        stream.onNext(publishRequest(1, "orders.*"));
        assertThat(connection.isPaused()).isFalse();

        // 2nd publish fills orders.slow to HWM 2
        stream.onNext(publishRequest(2, "orders.*"));
        assertThat(connection.isPaused()).isTrue();
    }

    @Test
    @DisplayName("Deleting a blocked queue immediately unblocks paused publisher")
    @SuppressWarnings("unchecked")
    void deletedQueueUnblocksProducerTest() {
        declarationService.declare("ephemeral.queue", TestQueueSettings.withFlowControl(new FlowControlPolicy(true, 1, 0, 0, 0, 0)));

        ServerCallStreamObserver<PublisherResponse> serverCall = mock(ServerCallStreamObserver.class);
        StreamObserver<PublisherRequest> stream = grpcPublisherService.openPublisherChannel(serverCall);

        PublisherConnection connection = publisherRegistry.activeConnections().iterator().next();

        stream.onNext(publishRequest(1, "ephemeral.queue"));
        assertThat(connection.isPaused()).isTrue();

        // Delete queue
        declarationService.delete("ephemeral.queue");

        awaitCondition(() -> !connection.isPaused());
        assertThat(connection.isPaused()).isFalse();
    }

    @Test
    @DisplayName("Overdue timeout on paused publisher causes next publication to be rejected with RESOURCE_EXHAUSTED")
    @SuppressWarnings("unchecked")
    void overdueTimeoutRejectionTest() {
        declarationService.declare("stall.queue", TestQueueSettings.withFlowControl(new FlowControlPolicy(true, 1, 0, 0, 0, 50)));

        ServerCallStreamObserver<PublisherResponse> serverCall = mock(ServerCallStreamObserver.class);
        List<PublisherResponse> responses = new CopyOnWriteArrayList<>();
        doAnswer(inv -> {
            responses.add(inv.getArgument(0));
            return null;
        }).when(serverCall).onNext(any());

        StreamObserver<PublisherRequest> stream = grpcPublisherService.openPublisherChannel(serverCall);
        PublisherConnection connection = publisherRegistry.activeConnections().iterator().next();

        stream.onNext(publishRequest(1, "stall.queue"));
        assertThat(connection.isPaused()).isTrue();

        // Trigger overdue notification
        connection.flowGate().notifyOverdue();
        assertThat(connection.flowGate().isOverdue()).isTrue();

        // Next publish should receive NACK RESOURCE_EXHAUSTED
        stream.onNext(publishRequest(2, "stall.queue"));

        assertThat(responses).anyMatch(r ->
                r.hasNack() && r.getNack().getError().getCode() == ErrorCode.RESOURCE_EXHAUSTED
        );
    }

    private static void awaitCondition(java.util.function.BooleanSupplier supplier) {
        long deadline = System.currentTimeMillis() + 2000;
        while (!supplier.getAsBoolean() && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private static PublisherRequest publishRequest(long id, String routingKey) {
        return PublisherRequest.newBuilder()
                .setPublish(PublishAction.newBuilder()
                        .setClientPublishId(id)
                        .setRoutingKey(routingKey)
                        .setPayload(ByteString.copyFrom("payload", StandardCharsets.UTF_8))
                        .build())
                .build();
    }
}
