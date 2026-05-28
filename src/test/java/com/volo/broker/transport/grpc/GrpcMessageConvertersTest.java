package com.volo.broker.transport.grpc;

import com.custombroker.grpc.Message;
import com.custombroker.grpc.PublishAction;
import com.custombroker.grpc.PublisherRequest;
import com.custombroker.grpc.PublisherResponse;
import com.google.protobuf.ByteString;
import com.volo.broker.core.command.model.PublishCommand;
import com.volo.broker.core.command.model.PublishResult;
import com.volo.broker.domain.message.DeliveryGuarantee;
import com.volo.broker.domain.message.QueuedMessage;
import com.volo.broker.transport.grpc.mapper.GrpcMessageConverters;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GrpcMessageConvertersTest {

    @Test
    void testToPublishCommand() {
        Map<String, String> headers = new HashMap<>();
        headers.put("content-type", "application/json");
        byte[] payload = "{\"test\": true}".getBytes();

        PublisherRequest request = PublisherRequest.newBuilder()
                .setPublish(PublishAction.newBuilder()
                        .setClientPublishId("client-1")
                        .setExchange("default")
                        .setRoutingKey("test.key")
                        .setPayload(ByteString.copyFrom(payload))
                        .putAllHeaders(headers)
                        .setDeliveryGuarantee(com.custombroker.grpc.DeliveryGuarantee.AT_LEAST_ONCE)
                        .build())
                .build();

        PublishCommand cmd = GrpcMessageConverters.toPublishCommand(request.getPublish());

        assertEquals("client-1", cmd.clientPublishId());
        assertEquals("default", cmd.exchange());
        assertEquals("test.key", cmd.routingKey());
        assertArrayEquals(payload, cmd.payload());
        assertEquals("application/json", cmd.headers().get("content-type"));
    }

    @Test
    void testToPublishResponse_Ack() {
        PublishResult result = PublishResult.ack(123);

        PublisherResponse response = GrpcMessageConverters.toPublisherResponse("client-1", result);

        assertTrue(response.hasAck());
        assertEquals("client-1", response.getAck().getClientPublishId());
        assertEquals(123, response.getAck().getMessageId());
    }

    @Test
    void testToPublishResponse_Nack() {
        PublishResult result = PublishResult.nack(com.volo.broker.core.error.BrokerErrorCode.INVALID_REQUEST, "Invalid payload");

        PublisherResponse response = GrpcMessageConverters.toPublisherResponse("client-2", result);

        assertTrue(response.hasNack());
        assertEquals("client-2", response.getNack().getClientPublishId());
        assertEquals("Invalid payload", response.getNack().getError().getMessage());
        assertEquals(com.custombroker.grpc.ErrorCode.INVALID_REQUEST, response.getNack().getError().getCode());
    }

    @Test
    void testToPongResponse() {
        PublisherResponse response = GrpcMessageConverters.toPongResponse();

        assertTrue(response.hasPong());
        assertTrue(response.getPong().getTimestamp() > 0);
    }

    @Test
    void testToProtoMessage() {
        Map<String, String> headers = new HashMap<>();
        headers.put("x-custom", "value");
        long messageId = 999;
        byte[] payload = "binary data".getBytes();
        String routingKey = "my.routing.key";
        long timestamp = System.currentTimeMillis();

        QueuedMessage qm = new QueuedMessage(messageId, payload, headers, routingKey, timestamp, DeliveryGuarantee.AT_LEAST_ONCE, false, 0);

        Message msg = GrpcMessageConverters.toProtoMessage(qm);

        assertEquals(messageId, msg.getId());
        assertArrayEquals(payload, msg.getPayload().toByteArray());
        assertEquals(routingKey, msg.getRoutingKey());
        assertEquals("value", msg.getHeadersMap().get("x-custom"));
        assertEquals(timestamp, msg.getTimestamp());
    }
}
