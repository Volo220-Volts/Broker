package com.volo.broker.transport.grpc.mapper;

import com.custombroker.grpc.ErrorCode;
import com.custombroker.grpc.ErrorInfo;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.error.BrokerException;

import java.util.Map;

public final class GrpcErrorMapper {
    private GrpcErrorMapper() {
    }

    public static ErrorInfo toProto(BrokerException exception) {
        return ErrorInfo.newBuilder()
                .setCode(toProtoCode(exception.code()))
                .setMessage(safeMessage(exception.getMessage()))
                .putAllDetails(exception.details())
                .build();
    }

    public static ErrorInfo fromCode(BrokerErrorCode code, String message) {
        return ErrorInfo.newBuilder()
                .setCode(toProtoCode(code))
                .setMessage(safeMessage(message))
                .build();
    }

    public static ErrorInfo internalError(Throwable throwable) {
        return ErrorInfo.newBuilder()
                .setCode(ErrorCode.INTERNAL_ERROR)
                .setMessage("Internal broker error")
                .putAllDetails(Map.of("exception", throwable.getClass().getSimpleName()))
                .build();
    }

    private static ErrorCode toProtoCode(BrokerErrorCode code) {
        if (code == null) {
            return ErrorCode.ERROR_CODE_UNSPECIFIED;
        }

        return switch (code) {
            case INVALID_REQUEST -> ErrorCode.INVALID_REQUEST;
            case PAYLOAD_TOO_LARGE -> ErrorCode.PAYLOAD_TOO_LARGE;
            case UNSUPPORTED_DELIVERY_GUARANTEE -> ErrorCode.UNSUPPORTED_DELIVERY_GUARANTEE;
            case INTERNAL_ERROR -> ErrorCode.INTERNAL_ERROR;
        };
    }

    private static String safeMessage(String message) {
        return message == null || message.isBlank() ? "Unknown error" : message;
    }
}
