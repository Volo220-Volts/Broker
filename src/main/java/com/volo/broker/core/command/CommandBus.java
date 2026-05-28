package com.volo.broker.core.command;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

@Component
public class CommandBus {
    private final Map<Class<? extends TransportCommand>, CommandHandler<?, ?>> handlers = new HashMap<>();

    public CommandBus(List<CommandHandler<?, ?>> handlers){
        for (CommandHandler<?, ?> handler : handlers) {
            this.handlers.put(handler.getCommandType(), handler);
        }
    }

    @SuppressWarnings("unchecked")
    public <C extends TransportCommand, R> R execute(C command){
        CommandHandler<C, R> handler = (CommandHandler<C, R>) handlers.get(command.getClass());

        if (handler == null) {
            throw new IllegalStateException("No handler found for command type: " + command.getClass());
        }

        return handler.handle(command);
    }
}
