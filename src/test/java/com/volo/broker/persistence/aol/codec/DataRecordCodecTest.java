package com.volo.broker.persistence.aol.codec;

import com.volo.broker.broker.message.QueuedMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.volo.broker.persistence.aol.codec.DataRecordCodec.CURRENT_VERSION;
import static com.volo.broker.persistence.aol.codec.DataRecordCodec.FIXED_BODY_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataRecordCodecTest {

    private DataRecordCodec codec;

    @BeforeEach
    void setUp() {
        codec = new DataRecordCodec();
    }

    // ======================================================================
    // Round-trip
    // ======================================================================

    @Nested
    @DisplayName("encode → decode round-trip")
    class RoundTrip {

        @Test
        void minimal_message_is_decoded_correctly() {
            QueuedMessage original = msg(1L, "q.orders", Map.of(), "hello".getBytes(), false, 0);

            DecodedRecord result = codec.decode(codec.encode(original, 42L));

            assertThat(result.queueOffset()).isEqualTo(42L);
            assertMessageEquals(original, result.message());
        }

        @Test
        void message_with_multiple_headers_is_decoded_correctly() {
            Map<String, String> headers = Map.of(
                    "x-trace-id", "abc-123",
                    "x-tenant",   "acme",
                    "x-priority", "high"
            );
            QueuedMessage original = msg(99L, "finance.payments", headers, new byte[]{1, 2, 3}, true, 3);

            DecodedRecord result = codec.decode(codec.encode(original, 0L));

            assertThat(result.queueOffset()).isZero();
            assertMessageEquals(original, result.message());
        }

        @Test
        void large_payload_survives_round_trip() {
            byte[] payload = new byte[1024 * 1024];
            payload[0] = 0x42;
            payload[payload.length - 1] = (byte) 0xFF;
            QueuedMessage original = msg(7L, "big.messages", Map.of(), payload, false, 0);

            DecodedRecord result = codec.decode(codec.encode(original, 10_000L));

            assertThat(result.queueOffset()).isEqualTo(10_000L);
            assertThat(result.message().getPayload()).isEqualTo(payload);
        }

        @Test
        void utf8_multibyte_routing_key_and_headers_are_preserved() {
            QueuedMessage original = msg(
                    3L, "черга.замовлення",
                    Map.of("ключ", "значення"),
                    "payload".getBytes(), false, 0
            );

            DecodedRecord result = codec.decode(codec.encode(original, 1L));

            assertThat(result.message().getRoutingKey()).isEqualTo("черга.замовлення");
            assertThat(result.message().getHeaders().get("ключ")).isEqualTo("значення");
        }

        @Test
        void empty_payload_round_trips_correctly() {
            QueuedMessage original = msg(5L, "q", Map.of(), new byte[0], false, 0);

            DecodedRecord result = codec.decode(codec.encode(original, 9L));

            assertThat(result.message().getPayload()).isEmpty();
        }

        @Test
        void redelivered_flag_and_delivery_attempt_are_preserved() {
            QueuedMessage original = msg(1L, "q", Map.of(), new byte[]{1}, true, 5);

            DecodedRecord result = codec.decode(codec.encode(original, 0L));

            assertThat(result.message().isRedelivered()).isTrue();
            assertThat(result.message().getDeliveryAttempt()).isEqualTo(5);
        }

        @Test
        void three_sequential_records_in_one_buffer_are_decoded_in_order() {
            QueuedMessage m1 = msg(1L, "q.a", Map.of(), "first".getBytes(),  false, 0);
            QueuedMessage m2 = msg(2L, "q.b", Map.of(), "second".getBytes(), false, 0);
            QueuedMessage m3 = msg(3L, "q.c", Map.of(), "third".getBytes(),  false, 0);

            ByteBuffer combined = join(
                    codec.encode(m1, 0L),
                    codec.encode(m2, 1L),
                    codec.encode(m3, 2L)
            );

            DecodedRecord r1 = codec.decode(combined);
            DecodedRecord r2 = codec.decode(combined);
            DecodedRecord r3 = codec.decode(combined);

            assertThat(r1.queueOffset()).isZero();
            assertThat(r2.queueOffset()).isEqualTo(1L);
            assertThat(r3.queueOffset()).isEqualTo(2L);
            assertMessageEquals(m1, r1.message());
            assertMessageEquals(m2, r2.message());
            assertMessageEquals(m3, r3.message());
            assertThat(combined.hasRemaining()).isFalse();
        }
    }

    // ======================================================================
    // Binary format
    // ======================================================================

    @Nested
    @DisplayName("binary format")
    class BinaryFormat {

        @Test
        void record_length_field_equals_remaining_bytes_after_it() {
            ByteBuffer buf = codec.encode(msg(1L, "q", Map.of(), new byte[]{42}, false, 0), 0L).duplicate();

            int recordLength = buf.getInt();

            assertThat(buf.remaining()).isEqualTo(recordLength);
        }

        @Test
        void version_byte_is_current_version() {
            ByteBuffer buf = codec.encode(minimalMsg(), 0L).duplicate();
            buf.getInt(); // skip recordLength

            assertThat(buf.get()).isEqualTo(CURRENT_VERSION);
        }

        @Test
        void queue_offset_is_at_the_correct_position() {
            long expectedOffset = 123_456_789L;
            ByteBuffer buf = codec.encode(minimalMsg(), expectedOffset).duplicate();
            buf.getInt(); // recordLength
            buf.get();    // version
            buf.get();    // flags

            assertThat(buf.getLong()).isEqualTo(expectedOffset);
        }

        @Test
        void compute_encoded_size_matches_actual_buffer_size() {
            QueuedMessage msg = msg(9L, "routing.key",
                    Map.of("k1", "v1", "k2", "v2"), new byte[512], false, 0);

            assertThat(codec.computeEncodedSize(msg)).isEqualTo(codec.encode(msg, 0L).remaining());
        }

        @Test
        void minimum_record_size_is_47_bytes() {
            QueuedMessage msg = msg(1L, "", Map.of(), new byte[0], false, 0);

            // 4 (recordLength) + 39 (fixed body) + 4 (crc)
            assertThat(codec.computeEncodedSize(msg)).isEqualTo(4 + FIXED_BODY_SIZE + 4);
        }
    }

    // ======================================================================
    // CRC32C integrity
    // ======================================================================

    @Nested
    @DisplayName("CRC32C integrity")
    class Integrity {

        @Test
        void flipping_payload_byte_causes_checksum_mismatch_exception() {
            byte[] bytes = toArray(codec.encode(minimalMsg(), 0L));
            bytes[bytes.length - 5] ^= 0xFF; // flip byte just before CRC

            assertThatThrownBy(() -> codec.decode(ByteBuffer.wrap(bytes)))
                    .isInstanceOf(CodecException.ChecksumMismatchException.class);
        }

        @Test
        void flipping_header_byte_causes_checksum_mismatch_exception() {
            byte[] bytes = toArray(codec.encode(
                    msg(1L, "q", Map.of("key", "val"), new byte[]{1}, false, 0), 0L));
            bytes[4 + FIXED_BODY_SIZE + 5] ^= 0x01;

            assertThatThrownBy(() -> codec.decode(ByteBuffer.wrap(bytes)))
                    .isInstanceOf(CodecException.ChecksumMismatchException.class);
        }

        @Test
        void encoding_is_deterministic() {
            QueuedMessage msg = msg(1L, "q", Map.of("k", "v"), new byte[]{7, 8, 9}, false, 0);

            assertThat(toArray(codec.encode(msg, 5L))).isEqualTo(toArray(codec.encode(msg, 5L)));
        }
    }

    // ======================================================================
    // Validation
    // ======================================================================

    @Nested
    @DisplayName("validation")
    class Validation {

        @Test
        void null_message_throws_null_pointer_exception() {
            assertThatThrownBy(() -> codec.encode(null, 0L))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void unknown_version_byte_throws_unsupported_version_exception() {
            byte[] bytes = toArray(codec.encode(minimalMsg(), 0L));
            bytes[4] = (byte) 99; // overwrite version byte

            assertThatThrownBy(() -> codec.decode(ByteBuffer.wrap(bytes)))
                    .isInstanceOf(CodecException.UnsupportedVersionException.class);
        }

        @Test
        void too_small_record_length_throws_codec_exception() {
            byte[] bytes = new byte[50];
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            buf.putInt(0); // recordLength = 0
            buf.flip();

            assertThatThrownBy(() -> codec.decode(buf))
                    .isInstanceOf(CodecException.class);
        }
    }

    // ======================================================================
    // Helpers
    // ======================================================================

    private QueuedMessage minimalMsg() {
        return msg(1L, "q", Map.of(), new byte[]{0x42}, false, 0);
    }

    private static QueuedMessage msg(long id, String routingKey, Map<String, String> headers,
                                     byte[] payload, boolean redelivered, int attempts) {
        return new QueuedMessage(id, payload, headers, routingKey,
                System.currentTimeMillis(), redelivered, attempts);
    }

    private static void assertMessageEquals(QueuedMessage expected, QueuedMessage actual) {
        assertThat(actual.getMessageId())       .isEqualTo(expected.getMessageId());
        assertThat(actual.getRoutingKey())      .isEqualTo(expected.getRoutingKey());
        assertThat(actual.getCreatedAt())       .isEqualTo(expected.getCreatedAt());
        assertThat(actual.isRedelivered())      .isEqualTo(expected.isRedelivered());
        assertThat(actual.getDeliveryAttempt()) .isEqualTo(expected.getDeliveryAttempt());
        assertThat(actual.getHeaders())         .isEqualTo(expected.getHeaders());
        assertThat(actual.getPayload())         .isEqualTo(expected.getPayload());
    }

    private static ByteBuffer join(ByteBuffer... buffers) {
        int total = 0;
        for (ByteBuffer b : buffers) total += b.remaining();
        ByteBuffer combined = ByteBuffer.allocate(total);
        for (ByteBuffer b : buffers) combined.put(b.duplicate());
        combined.flip();
        return combined;
    }

    private static byte[] toArray(ByteBuffer buf) {
        byte[] bytes = new byte[buf.remaining()];
        buf.duplicate().get(bytes);
        return bytes;
    }
}
