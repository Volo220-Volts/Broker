package com.volo.broker.transport.grpc;

import com.custombroker.grpc.BrokerResponse;
import com.volo.broker.broker.message.DeliveryGuarantee;
import com.volo.broker.broker.message.MessageDelivery;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.transport.grpc.adapter.GrpcConsumerChannelAdapter;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class GrpcConsumerChannelAdapterTest {

    @Mock
    private StreamObserver<BrokerResponse> responseObserver;

    private GrpcConsumerChannelAdapter adapter;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        adapter = new GrpcConsumerChannelAdapter(responseObserver);
    }

    @Test
    void testConsumerTag_NotEmpty() {
        
        String tag = adapter.consumerTag();

        
        assertNotNull(tag);
        assertFalse(tag.isEmpty());
    }

    @Test
    void testConsumerTag_ConsistentAcrossCalls() {
        
        String tag1 = adapter.consumerTag();
        String tag2 = adapter.consumerTag();

        
        assertEquals(tag1, tag2);
    }

    @Test
    void testCanAcceptMore_InitiallyTrue() {
        
        boolean canAccept = adapter.canAcceptMore();

        
        assertTrue(canAccept);
    }

    @Test
    void testCanAcceptMore_FalseAfterClose() {
        
        adapter.close("test-close");

        
        boolean canAccept = adapter.canAcceptMore();

        
        assertFalse(canAccept);
    }

    @Test
    void testSend_DeliveryToResponse() {
        
        Map<String, String> headers = new HashMap<>();
        headers.put("x-header", "value");
        byte[] payload = "test-message".getBytes();

        QueuedMessage qm = new QueuedMessage(
                100,
                payload,
                headers,
                "test.key",
                System.currentTimeMillis(),
                DeliveryGuarantee.AT_LEAST_ONCE,
                false,
                0
        );
        MessageDelivery delivery = MessageDelivery.of(42, qm);

        
        adapter.send(delivery);

        
        ArgumentCaptor<BrokerResponse> captor = ArgumentCaptor.forClass(BrokerResponse.class);
        verify(responseObserver, times(1)).onNext(captor.capture());
        
        BrokerResponse response = captor.getValue();
        assertTrue(response.hasDelivery());
        assertEquals(42, response.getDelivery().getDeliveryTag());
        assertEquals(100, response.getDelivery().getMessage().getId());
        assertEquals("test.key", response.getDelivery().getMessage().getRoutingKey());
    }

    @Test
    void testSend_MultipleDeliveries() {
        
        QueuedMessage qm1 = new QueuedMessage(1, "msg1".getBytes(), null, "key1", 1000, DeliveryGuarantee.AT_LEAST_ONCE, false, 0);
        QueuedMessage qm2 = new QueuedMessage(2, "msg2".getBytes(), null, "key2", 2000, DeliveryGuarantee.AT_LEAST_ONCE, false, 0);

        MessageDelivery del1 = MessageDelivery.of(10, qm1);
        MessageDelivery del2 = MessageDelivery.of(11, qm2);

        
        adapter.send(del1);
        adapter.send(del2);

        
        ArgumentCaptor<BrokerResponse> captor = ArgumentCaptor.forClass(BrokerResponse.class);
        verify(responseObserver, times(2)).onNext(captor.capture());

        java.util.List<BrokerResponse> responses = captor.getAllValues();
        assertEquals(2, responses.size());
        assertEquals(10, responses.get(0).getDelivery().getDeliveryTag());
        assertEquals(11, responses.get(1).getDelivery().getDeliveryTag());
    }

    @Test
    void testSend_IgnoredAfterClose() {
        
        adapter.close("closed");
        QueuedMessage qm = new QueuedMessage(1, new byte[0], null, "key", 1000, DeliveryGuarantee.AT_LEAST_ONCE, false, 0);
        MessageDelivery delivery = MessageDelivery.of(99, qm);

        
        adapter.send(delivery);

        
        verify(responseObserver, times(0)).onNext(any());
    }

    @Test
    void testClose_SendsCompletionSignal() {
        
        adapter.close("test-reason");

        
        verify(responseObserver, times(0)).onNext(any()); 
        verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void testClose_Idempotent() {
        
        adapter.close("first");
        adapter.close("second");

        
        verify(responseObserver, times(1)).onCompleted(); 
    }

    @Test
    void testOnAck_NoOp() {
        
        adapter.onAck(42);

        
        verify(responseObserver, times(0)).onNext(any());
    }

    @Test
    void testOnNack_NoOp() {
        
        adapter.onNack(42, true);

        
        verify(responseObserver, times(0)).onNext(any());
    }

    @Test
    void testSend_WithStreamException() {
        
        doThrow(new RuntimeException("Stream broken")).when(responseObserver).onNext(any());
        QueuedMessage qm = new QueuedMessage(1, new byte[0], null, "key", 1000, DeliveryGuarantee.AT_LEAST_ONCE, false, 0);
        MessageDelivery delivery = MessageDelivery.of(50, qm);

        
        adapter.send(delivery);

        
        assertTrue(!adapter.canAcceptMore());
        verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void testDifferentAdapters_DifferentTags() {
        
        GrpcConsumerChannelAdapter adapter1 = new GrpcConsumerChannelAdapter(responseObserver);
        GrpcConsumerChannelAdapter adapter2 = new GrpcConsumerChannelAdapter(responseObserver);

        
        String tag1 = adapter1.consumerTag();
        String tag2 = adapter2.consumerTag();

        
        assertNotEquals(tag1, tag2);
    }
}
