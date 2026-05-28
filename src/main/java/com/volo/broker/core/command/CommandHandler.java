package com.volo.broker.core.command;

public interface CommandHandler<C extends TransportCommand, R> {
    Class<C> getCommandType();
    
    R handle(C command);
}
