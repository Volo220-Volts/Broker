package com.volo.broker.broker.command.handler;

import com.volo.broker.broker.command.model.PublishCommand;
import com.volo.broker.broker.command.model.PublishResult;
import com.volo.broker.broker.services.PublishService;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class PublishCommandHandlerTest {

    private final PublishService publishService = mock(PublishService.class);
    private final PublishCommandHandler handler = new PublishCommandHandler(publishService);

    @Test
    void handle_shouldDelegateToPublishService() {
        PublishCommand command = new PublishCommand(
                1L,
                "orders.created",
                "payload".getBytes(),
                Map.of()
        );

        PublishResult expected = PublishResult.ack(1L, 1);
        when(publishService.publish(command)).thenReturn(expected);

        PublishResult result = handler.handle(command);

        assertThat(result).isSameAs(expected);
        verify(publishService).publish(command);
    }
}