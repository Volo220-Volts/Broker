package com.volo.broker.transport.grpc;

import com.custombroker.grpc.PublishRequest;
import com.custombroker.grpc.PublishResponse;
import com.custombroker.grpc.PublishResponseSummary;
import com.custombroker.grpc.Message;
import com.volo.broker.transport.model.PublishCommand;
import com.volo.broker.transport.model.PublishResult;
import com.volo.broker.engine.QueuedMessage;
import com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.Map;

public class GrpcMessageConverters {
    public static PublishCommand toPublishCommand(PublishRequest req) {
        Map<String, String> headers = new HashMap<>(req.getHeadersMap());
        byte[] payload = req.getPayload() == null ? new byte[0] : req.getPayload().toByteArray();
        return new PublishCommand(req.getRoutingKey(), payload, headers);
    }

    public static PublishResponse toPublishResponse(PublishResult res) {
        PublishResponse.Builder b = PublishResponse.newBuilder();
        b.setSuccess(res.success);
        if (res.messageId >= -1) b.setMessageId(res.messageId);
        if (res.errorMessage != null) b.setErrorMessage(res.errorMessage);
        return b.build();
    }

    public static PublishResponseSummary toPublishSummary(long total, boolean success) {
        return PublishResponseSummary.newBuilder().setTotalPublished(total).setSuccess(success).build();
    }

    public static Message toProtoMessage(QueuedMessage qm) {
        Message.Builder b = Message.newBuilder();
        b.setId(qm.getMessageId());
        b.setPayload(ByteString.copyFrom(qm.getPayload()));
        if (qm.getRoutingKey() != null) b.setRoutingKey(qm.getRoutingKey());
        b.putAllHeaders(qm.getHeaders());
        b.setTimestamp(qm.getCreatedAt());
        return b.build();
    }
}
