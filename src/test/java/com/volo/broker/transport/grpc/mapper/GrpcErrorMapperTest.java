package com.volo.broker.transport.grpc.mapper;

import com.custombroker.grpc.ErrorCode;
import com.custombroker.grpc.ErrorInfo;
import com.volo.broker.broker.error.BrokerErrorCode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcErrorMapperTest {

    @Test
    void fromCode_shouldMapInvalidRequest() {
        ErrorInfo error = GrpcErrorMapper.fromCode(
                BrokerErrorCode.INVALID_REQUEST,
                "Invalid request"
        );

        assertThat(error.getCode()).isEqualTo(ErrorCode.INVALID_REQUEST);
        assertThat(error.getMessage()).isEqualTo("Invalid request");
    }

    @Test
    void fromCode_shouldMapRouteNotFound() {
        ErrorInfo error = GrpcErrorMapper.fromCode(
                BrokerErrorCode.QUEUE_NOT_FOUND,
                "Queue not found"
        );

        assertThat(error.getCode()).isEqualTo(ErrorCode.QUEUE_NOT_FOUND);
    }

    @Test
    void fromCode_shouldMapQueueAlreadyExists() {
        ErrorInfo error = GrpcErrorMapper.fromCode(
                BrokerErrorCode.QUEUE_ALREADY_EXISTS,
                "Queue already exists"
        );

        assertThat(error.getCode()).isEqualTo(ErrorCode.QUEUE_ALREADY_EXISTS);
    }

    @Test
    void fromCode_shouldMapQueueNotFound() {
        ErrorInfo error = GrpcErrorMapper.fromCode(
                BrokerErrorCode.QUEUE_NOT_FOUND,
                "Queue not found"
        );

        assertThat(error.getCode()).isEqualTo(ErrorCode.QUEUE_NOT_FOUND);
    }

    @Test
    void fromCode_shouldMapQueueOverflow() {
        ErrorInfo error = GrpcErrorMapper.fromCode(
                BrokerErrorCode.QUEUE_OVERFLOW,
                "Queue overflow"
        );

        assertThat(error.getCode()).isEqualTo(ErrorCode.QUEUE_OVERFLOW);
    }

    @Test
    void fromCode_shouldMapInternalErrorWhenCodeIsNull() {
        ErrorInfo error = GrpcErrorMapper.fromCode(
                null,
                "Unknown error"
        );

        assertThat(error.getCode()).isEqualTo(ErrorCode.INTERNAL_ERROR);
        assertThat(error.getMessage()).isEqualTo("Unknown error");
    }
}