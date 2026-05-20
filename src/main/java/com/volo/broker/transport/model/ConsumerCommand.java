package com.volo.broker.transport.model;

public sealed interface ConsumerCommand permits SubscribeCommand, AckCommand, NackCommand {}
