package com.volo.broker.transport.grpc;

import com.custombroker.grpc.BrokerResponse;
import com.custombroker.grpc.ConsumerRequest;
import com.custombroker.grpc.ConsumerServiceGrpc;
import com.custombroker.grpc.SubscribeAction;
import com.custombroker.grpc.AckAction;
import com.custombroker.grpc.NackAction;
import io.grpc.stub.StreamObserver;

import java.util.logging.Logger;

import org.springframework.stereotype.Component;
import com.volo.broker.transport.TransportCommandRouter;
import com.volo.broker.transport.model.SubscribeCommand;
import com.volo.broker.transport.model.AckCommand;
import com.volo.broker.transport.model.NackCommand;
import com.volo.broker.engine.QueueManager;
import com.volo.broker.engine.SubscriptionManager;

@Component
public class GrpcConsumerService extends ConsumerServiceGrpc.ConsumerServiceImplBase {
    private final TransportCommandRouter router;
    private final SubscriptionManager subscriptionManager;
    private final Logger logger = Logger.getLogger(GrpcConsumerService.class.getName());
    private final QueueManager queueManager;

    public GrpcConsumerService(TransportCommandRouter router, SubscriptionManager subscriptionManager, QueueManager queueManager) {
        this.router = router;
        this.subscriptionManager = subscriptionManager;
        this.queueManager = queueManager;
    }

    @Override
    public StreamObserver<ConsumerRequest> openChannel(StreamObserver<BrokerResponse> responseObserver) {
        GrpcConsumerChannelAdapter adapter = new GrpcConsumerChannelAdapter(responseObserver);

        return new StreamObserver<ConsumerRequest>() {
            @Override
            public void onNext(ConsumerRequest request) {
                try {
                    if (request.hasSubscribe()) {
                        SubscribeAction sa = request.getSubscribe();

                        SubscribeCommand cmd = new SubscribeCommand(
                                sa.getQueueName(),
                                sa.getPrefetchCount(),
                                adapter.consumerTag(),
                                adapter
                        );

                        router.route(cmd);
                        logger.info("Consumer subscribed: " + adapter.consumerTag());

                    } else if (request.hasAck()) {
                        AckAction a = request.getAck();

                        router.route(new AckCommand(
                                adapter.consumerTag(),
                                a.getDeliveryTag(),
                                a.getMessageId()
                        ));

                    } else if (request.hasNack()) {
                        NackAction n = request.getNack();

                        router.route(new NackCommand(
                                adapter.consumerTag(),
                                n.getDeliveryTag(),
                                n.getMessageId(),
                                n.getRequeue()
                        ));
                    }

                } catch (Exception ex) {
                    cleanup();
                    adapter.close("request-processing-error: " + ex.getMessage());
                }
            }

            @Override
            public void onError(Throwable t) {
                cleanup();
            }

            @Override
            public void onCompleted() {
                cleanup();
                adapter.close("client-completed");
            }

            private void cleanup() {
                try {
                    var s = subscriptionManager.unregister(adapter.consumerTag());
                    if(s.isPresent()){
                        logger.info("Consumer unsubscribed: " + adapter.consumerTag());
                        if(s.isPresent()){
                            var drained = s.get().getInFlightTracker().drainAll().stream().toList();
                            for(var i = drained.size() - 1; i >= 0; i--){
                                var d = drained.get(i);
                                queueManager.requeue(s.get().queueName(), d);
                            }
                            
                        }
                    }
                } catch (Throwable ignored) {
                }
            }
        };
    }
}
