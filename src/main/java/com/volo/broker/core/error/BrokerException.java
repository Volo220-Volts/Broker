package com.volo.broker.core.error;

import java.util.Map;
import java.util.Objects;

public class BrokerException extends RuntimeException {
    private final BrokerErrorCode code;
    private final Map<String, String> details;

    public BrokerException(BrokerErrorCode code, String message) {
        this(code, message, Map.of(), null);
    }

    public BrokerException(BrokerErrorCode code, String message, Map<String, String> details) {
        this(code, message, details, null);
    }

    public BrokerException(BrokerErrorCode code, String message, Map<String, String> details, Throwable cause) {
        super(message, cause);
        this.code = Objects.requireNonNull(code, "code must not be null");
        this.details = details == null ? Map.of() : Map.copyOf(details);
    }

    public BrokerErrorCode code() {
        return code;
    }

    public Map<String, String> details() {
        return details;
    }
}
