package com.volo.broker.broker.command;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CommandBusTest {

    @Test
    void execute_shouldDispatchCommandToMatchingHandler() {
        TestCommandHandler handler = new TestCommandHandler();
        CommandBus commandBus = new CommandBus(List.of(handler));

        String result = commandBus.execute(new TestCommand("hello"));

        assertThat(result).isEqualTo("handled: hello");
    }

    @Test
    void execute_shouldThrowWhenHandlerIsMissing() {
        CommandBus commandBus = new CommandBus(List.of());

        assertThatThrownBy(() -> commandBus.execute(new TestCommand("hello")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("handler");
    }

    private record TestCommand(String value) implements TransportCommand {
    }

    private static class TestCommandHandler implements CommandHandler<TestCommand, String> {

        @Override
        public Class<TestCommand> getCommandType() {
            return TestCommand.class;
        }

        @Override
        public String handle(TestCommand command) {
            return "handled: " + command.value();
        }
    }
}