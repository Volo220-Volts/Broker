package com.volo.broker.transport.grpc.mapper;

import com.custombroker.grpc.Message;
import com.custombroker.grpc.PublishAction;
import com.custombroker.grpc.PublisherRequest;
import com.custombroker.grpc.PublisherResponse;
import com.volo.broker.broker.command.model.PublishCommand;
import com.volo.broker.broker.command.model.PublishResult;
import com.volo.broker.broker.error.BrokerException;
import com.volo.broker.broker.message.QueuedMessage;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpcMessageConvertersTest {

    @Test
    void toPublishCommand_shouldConvertValidPublishAction() {
        PublishAction action = PublishAction.newBuilder()
                .setClientPublishId(1L)
                .setRoutingKey("orders.created")
                .setPayload(ByteString.copyFrom("payload", StandardCharsets.UTF_8))
                .putHeaders("content-type", "text/plain")
                .build();

        PublishCommand command = GrpcMessageConverters.toPublishCommand(action);

        assertThat(command.clientPublishId()).isEqualTo(1L);
        assertThat(command.routingKey()).isEqualTo("orders.created");
        assertThat(new String(command.payload(), StandardCharsets.UTF_8)).isEqualTo("payload");
        assertThat(command.headers()).containsEntry("content-type", "text/plain");
    }

    @Test
    void toPublishCommand_shouldRejectBlankRoutingKey() {
        PublishAction action = PublishAction.newBuilder()
                .setClientPublishId(1L)
                .setRoutingKey("")
                .setPayload(ByteString.copyFrom("payload", StandardCharsets.UTF_8))
                .build();

        assertThatThrownBy(() -> GrpcMessageConverters.toPublishCommand(action))
                .isInstanceOf(BrokerException.class);
    }

    @Test
    void toPublishCommand_shouldRejectEmptyPayload() {
        PublishAction action = PublishAction.newBuilder()
                .setClientPublishId(1L)
                .setRoutingKey("orders.created")
                .setPayload(ByteString.EMPTY)
                .build();

        assertThatThrownBy(() -> GrpcMessageConverters.toPublishCommand(action))
                .isInstanceOf(BrokerException.class);
    }


    @Test
    void testToPublishCommand() {
        Map<String, String> headers = new HashMap<>();
        headers.put("content-type", "application/json");
        byte[] payload = "{\"test\": true}".getBytes();

        PublisherRequest request = PublisherRequest.newBuilder()
                .setPublish(PublishAction.newBuilder()
                        .setClientPublishId(1L)
                        .setRoutingKey("test.key")
                        .setPayload(ByteString.copyFrom(payload))
                        .putAllHeaders(headers)
                        .build())
                .build();

        PublishCommand cmd = GrpcMessageConverters.toPublishCommand(request.getPublish());

        assertEquals(1L, cmd.clientPublishId());
        assertEquals("test.key", cmd.routingKey());
        assertArrayEquals(payload, cmd.payload());
        assertEquals("application/json", cmd.headers().get("content-type"));
    }

    @Test
    void testToPublishResponse_Ack() {
        PublishResult result = PublishResult.ack(1L, 1);

        PublisherResponse response = GrpcMessageConverters.toPublisherResponse(result);

        assertTrue(response.hasAck());
        assertEquals(1L, response.getAck().getClientPublishId());
        assertEquals(1, response.getAck().getRoutedQueueCount());
    }

    @Test
    void testToPublishResponse_Nack() {
        PublishResult result = PublishResult.nack(1L, com.volo.broker.broker.error.BrokerErrorCode.INVALID_REQUEST, "Invalid payload");

        PublisherResponse response = GrpcMessageConverters.toPublisherResponse(result);

        assertTrue(response.hasNack());
        assertEquals(1L, response.getNack().getClientPublishId());
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

        QueuedMessage qm = new QueuedMessage(messageId, payload, headers, routingKey, timestamp, false, 0);

        Message msg = GrpcMessageConverters.toProtoMessage(qm);

        assertEquals(messageId, msg.getId());
        assertArrayEquals(payload, msg.getPayload().toByteArray());
        assertEquals(routingKey, msg.getRoutingKey());
        assertEquals("value", msg.getHeadersMap().get("x-custom"));
        assertEquals(timestamp, msg.getTimestamp());
    }
}
