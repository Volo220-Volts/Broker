package com.volo.broker.transport.grpc;

import com.custombroker.grpc.PublishRequest;
import com.custombroker.grpc.PublishResponse;
import com.custombroker.grpc.PublishResponseSummary;
import com.custombroker.grpc.PublisherServiceGrpc;
import com.volo.broker.transport.TransportCommandRouter;
import com.volo.broker.transport.model.PublishCommand;
import com.volo.broker.transport.model.PublishResult;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Component;

@Component
public class GrpcPublisherService extends PublisherServiceGrpc.PublisherServiceImplBase {
    private final TransportCommandRouter router;
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GrpcPublisherService.class);

    public GrpcPublisherService(TransportCommandRouter router) {
        this.router = router;
    }

    // @Override
    // public void publish(PublishRequest request, StreamObserver<PublishResponse> responseObserver) {
    //     PublishCommand cmd = GrpcMessageConverters.toPublishCommand(request);
    //     PublishResult res = router.route(cmd);
    //     logger.info("Received publish request for queue '{}', result: {}, message: {}", request.getRoutingKey(), res.success, request.getPayload().toString());
    //     responseObserver.onNext(GrpcMessageConverters.toPublishResponse(res));
    //     responseObserver.onCompleted();
    // }

    @Override
    public void publish(PublishRequest request, StreamObserver<PublishResponse> responseObserver) {
        try {
            PublishCommand cmd = GrpcMessageConverters.toPublishCommand(request);
            PublishResult res = router.route(cmd);

            responseObserver.onNext(GrpcMessageConverters.toPublishResponse(res));
            responseObserver.onCompleted();

        } catch (Exception ex) {
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription("Publish failed")
                            .withCause(ex)
                            .asRuntimeException()
            );
        }
    }
    
    @Override
    public StreamObserver<PublishRequest> publishStream(StreamObserver<PublishResponseSummary> responseObserver) {
        return new StreamObserver<PublishRequest>() {
            long total = 0;
            boolean ok = true;

            @Override
            public void onNext(PublishRequest publishRequest) {
                PublishCommand cmd = GrpcMessageConverters.toPublishCommand(publishRequest);
                PublishResult res = router.route(cmd);
                total++;
                if (!res.success) ok = false;
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(GrpcMessageConverters.toPublishSummary(total, ok));
                responseObserver.onCompleted();
            }
        };
    }
}
