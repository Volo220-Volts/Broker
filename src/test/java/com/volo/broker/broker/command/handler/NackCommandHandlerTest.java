package com.volo.broker.broker.command.handler;

import com.volo.broker.broker.command.model.NackCommand;
import com.volo.broker.broker.services.ConsumerAcknowledgementService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class NackCommandHandlerTest {

    private final ConsumerAcknowledgementService acknowledgementService =
            mock(ConsumerAcknowledgementService.class);

    private final NackCommandHandler handler = new NackCommandHandler(acknowledgementService);

    @Test
    void handle_shouldDelegateToAcknowledgementService() {
        NackCommand command = new NackCommand(
                "consumer-1",
                10L,
                100L,
                true
        );

        handler.handle(command);

        verify(acknowledgementService).nack(command);
    }
}