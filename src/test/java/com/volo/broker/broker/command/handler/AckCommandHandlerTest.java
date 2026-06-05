package com.volo.broker.broker.command.handler;

import com.volo.broker.broker.command.model.AckCommand;
import com.volo.broker.broker.services.ConsumerAcknowledgementService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class AckCommandHandlerTest {

    private final ConsumerAcknowledgementService acknowledgementService =
            mock(ConsumerAcknowledgementService.class);

    private final AckCommandHandler handler = new AckCommandHandler(acknowledgementService);

    @Test
    void handle_shouldDelegateToAcknowledgementService() {
        AckCommand command = new AckCommand(
                "consumer-1",
                10L,
                100L
        );

        handler.handle(command);

        verify(acknowledgementService).ack(command);
    }
}