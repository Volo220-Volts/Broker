package com.volo.broker.transport.grpc.connection;

import com.custombroker.grpc.BrokerResponse;
import com.volo.broker.TestStreamObserver;
import com.volo.broker.broker.command.CommandBus;
import com.volo.broker.broker.command.model.DisconnectCommand;
import com.volo.broker.transport.grpc.SafeGrpcObserver;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ConsumerConnectionTest {

    @Test
    void close_shouldSendDisconnectCommand() {
        CommandBus commandBus = mock(CommandBus.class);

        ConsumerConnection connection = new ConsumerConnection(
                new SafeGrpcObserver<>(new TestStreamObserver<BrokerResponse>()),
                commandBus
        );

        connection.close();

        assertThat(connection.isClosed()).isTrue();

        verify(commandBus).execute(any(DisconnectCommand.class));
    }

    @Test
    void close_shouldSendDisconnectOnlyOnce() {
        CommandBus commandBus = mock(CommandBus.class);

        ConsumerConnection connection = new ConsumerConnection(
                new SafeGrpcObserver<>(new TestStreamObserver<BrokerResponse>()),
                commandBus
        );

        connection.close();
        connection.close();

        verify(commandBus, times(1)).execute(any(DisconnectCommand.class));
    }
}