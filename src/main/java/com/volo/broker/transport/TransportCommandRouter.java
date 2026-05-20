package com.volo.broker.transport;

import com.volo.broker.transport.model.AckCommand;
import com.volo.broker.transport.model.PublishCommand;
import com.volo.broker.transport.model.PublishResult;
import com.volo.broker.transport.model.NackCommand;
import com.volo.broker.transport.model.SubscribeCommand;

public interface TransportCommandRouter {
    PublishResult route(PublishCommand cmd);
    void route(SubscribeCommand cmd);
    void route(AckCommand cmd);
    void route(NackCommand cmd);
}
