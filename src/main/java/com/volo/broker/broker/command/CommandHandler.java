package com.volo.broker.broker.command;

public interface CommandHandler<C extends TransportCommand, R> {
    Class<C> getCommandType();
    
    R handle(C command);
}
