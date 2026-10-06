package com.volo.broker.persistence.aol.io;

import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.persistence.aol.codec.DataRecordCodec;
import com.volo.broker.persistence.aol.codec.DecodedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AolFileChannelTest {

    @TempDir
    Path tempDir;

    private DataRecordCodec codec;

    @BeforeEach
    void setUp() {
        codec = new DataRecordCodec();
    }

    // ======================================================================
    // Write + read round-trip
    // ======================================================================

    @Nested
    @DisplayName("write → read round-trip")
    class WriteRead {

        @Test
        void three_records_are_decoded_correctly_after_write_and_force() throws IOException {
            Path file = tempDir.resolve("roundtrip.log");
            List<QueuedMessage> originals = List.of(
                    msg(1L, "q.orders",   Map.of("x-env", "prod"), "first".getBytes(),  false, 0),
                    msg(2L, "q.payments", Map.of("x-env", "test"), "second".getBytes(), true,  2),
                    msg(3L, "q.emails",   Map.of(),                new byte[]{0x42},    false, 0)
            );

            List<Long> positions = new ArrayList<>();
            try (AolFileChannel seg = AolFileChannel.createNew(file)) {
                for (int i = 0; i < originals.size(); i++) {
                    positions.add(seg.write(codec.encode(originals.get(i), i)));
                }
                seg.force();
            }

            try (AolFileChannel seg = AolFileChannel.openExisting(file)) {
                for (int i = 0; i < originals.size(); i++) {
                    int sz = codec.computeEncodedSize(originals.get(i));
                    DecodedRecord decoded = codec.decode(seg.read(positions.get(i), sz));

                    assertThat(decoded.queueOffset()).isEqualTo(i);
                    assertMessageEquals(originals.get(i), decoded.message());
                }
            }
        }

        @Test
        void size_grows_by_encoded_size_after_each_write() throws IOException {
            Path file = tempDir.resolve("sizetrack.log");
            QueuedMessage m1 = msg(10L, "a", Map.of(), "hello".getBytes(),   false, 0);
            QueuedMessage m2 = msg(11L, "b", Map.of(), "world!!".getBytes(), false, 0);

            try (AolFileChannel seg = AolFileChannel.createNew(file)) {
                long expectedSize = 0;
                for (int i = 0; i < 2; i++) {
                    QueuedMessage m = i == 0 ? m1 : m2;
                    ByteBuffer encoded = codec.encode(m, i);
                    int encodedSize = encoded.remaining();
                    long retPos = seg.write(encoded);

                    assertThat(retPos).isEqualTo(expectedSize);
                    expectedSize += encodedSize;
                    assertThat(seg.size()).isEqualTo(expectedSize);
                }
            }
        }

        @Test
        void force_does_not_corrupt_written_data() throws IOException {
            Path file = tempDir.resolve("force.log");
            QueuedMessage msg = msg(99L, "q", Map.of("k", "v"), "payload".getBytes(), false, 0);
            int sz = codec.computeEncodedSize(msg);

            try (AolFileChannel seg = AolFileChannel.createNew(file)) {
                long pos0 = seg.write(codec.encode(msg, 0L));
                seg.force();
                long pos1 = seg.write(codec.encode(msg, 1L));
                seg.force();

                assertThat(codec.decode(seg.read(pos0, sz)).queueOffset()).isZero();
                assertThat(codec.decode(seg.read(pos1, sz)).queueOffset()).isEqualTo(1L);
            }
        }

        @Test
        void open_existing_resumes_at_eof_and_all_records_remain_readable() throws IOException {
            Path file = tempDir.resolve("reopen.log");
            QueuedMessage msg = msg(5L, "q", Map.of(), "data".getBytes(), false, 0);
            int sz = codec.computeEncodedSize(msg);

            try (AolFileChannel seg = AolFileChannel.createNew(file)) {
                seg.write(codec.encode(msg, 0L));
                seg.write(codec.encode(msg, 1L));
                seg.force();
            }

            try (AolFileChannel seg = AolFileChannel.openExisting(file)) {
                assertThat(seg.size()).isEqualTo((long) sz * 2);
                seg.write(codec.encode(msg, 2L));
                seg.force();

                for (int i = 0; i < 3; i++) {
                    DecodedRecord r = codec.decode(seg.read((long) i * sz, sz));
                    assertThat(r.queueOffset()).isEqualTo(i);
                }
            }
        }

        @Test
        void records_can_be_read_in_reverse_order_by_stored_position() throws IOException {
            Path file = tempDir.resolve("positions.log");

            record Written(long position, QueuedMessage msg, int size) {}
            List<Written> written = new ArrayList<>();

            try (AolFileChannel seg = AolFileChannel.createNew(file)) {
                for (int i = 0; i < 10; i++) {
                    QueuedMessage m = msg(i, "q." + i, Map.of("i", String.valueOf(i)),
                            ("p-" + i).getBytes(), false, 0);
                    int sz = codec.computeEncodedSize(m);
                    long pos = seg.write(codec.encode(m, i));
                    written.add(new Written(pos, m, sz));
                }
                seg.force();

                for (int i = written.size() - 1; i >= 0; i--) {
                    Written w = written.get(i);
                    DecodedRecord r = codec.decode(seg.read(w.position(), w.size()));
                    assertThat(r.queueOffset()).isEqualTo(i);
                }
            }
        }
    }

    // ======================================================================
    // Tail validator
    // ======================================================================

    @Nested
    @DisplayName("SegmentTailValidator")
    class TailValidation {

        private SegmentTailValidator validator;

        @BeforeEach
        void setUpValidator() {
            validator = new SegmentTailValidator(codec);
        }

        @Test
        void all_valid_records_are_found_and_no_truncation_needed() throws IOException {
            Path file = tempDir.resolve("valid.log");
            try (AolFileChannel seg = AolFileChannel.createNew(file)) {
                for (int i = 0; i < 5; i++) {
                    seg.write(codec.encode(msg(i, "q", Map.of(), ("msg" + i).getBytes(), false, 0), i));
                }
                seg.force();

                SegmentTailValidator.ValidationResult result = validator.validate(seg);

                assertThat(result.recordsFound()).isEqualTo(5);
                assertThat(result.truncationNeeded()).isFalse();
                assertThat(result.lastValidPosition()).isEqualTo(seg.size());
            }
        }

        @Test
        void partial_tail_record_is_detected_and_truncation_point_is_correct() throws IOException {
            Path file = tempDir.resolve("truncated.log");
            QueuedMessage msg = msg(1L, "q", Map.of(), "hello".getBytes(), false, 0);

            try (AolFileChannel seg = AolFileChannel.createNew(file)) {
                seg.write(codec.encode(msg, 0L));
                seg.write(codec.encode(msg, 1L));
                long validEnd = seg.size();

                // Inject half of a third record to simulate crash
                ByteBuffer partial = codec.encode(msg, 2L);
                byte[] halfBytes = new byte[partial.remaining() / 2];
                partial.get(halfBytes);
                seg.write(ByteBuffer.wrap(halfBytes));

                SegmentTailValidator.ValidationResult result = validator.validate(seg);

                assertThat(result.recordsFound()).isEqualTo(2);
                assertThat(result.truncationNeeded()).isTrue();
                assertThat(result.lastValidPosition()).isEqualTo(validEnd);
            }
        }

        @Test
        void truncate_repairs_corrupt_tail_and_revalidation_shows_clean_file() throws IOException {
            Path file = tempDir.resolve("repair.log");
            QueuedMessage msg = msg(1L, "q", Map.of(), "payload".getBytes(), false, 0);

            try (AolFileChannel seg = AolFileChannel.createNew(file)) {
                seg.write(codec.encode(msg, 0L));
                seg.write(codec.encode(msg, 1L));
                long validEnd = seg.size();

                seg.write(ByteBuffer.wrap(new byte[20])); // garbage

                SegmentTailValidator.ValidationResult before = validator.validate(seg);
                assertThat(before.truncationNeeded()).isTrue();

                seg.truncate(before.lastValidPosition());
                assertThat(seg.size()).isEqualTo(validEnd);

                SegmentTailValidator.ValidationResult after = validator.validate(seg);
                assertThat(after.truncationNeeded()).isFalse();
                assertThat(after.recordsFound()).isEqualTo(2);
            }
        }
    }

    // ======================================================================
    // Error cases
    // ======================================================================

    @Nested
    @DisplayName("error cases")
    class ErrorCases {

        @Test
        void read_beyond_eof_throws_eof_exception() throws IOException {
            Path file = tempDir.resolve("eof.log");
            try (AolFileChannel seg = AolFileChannel.createNew(file)) {
                seg.write(ByteBuffer.wrap(new byte[]{1, 2, 3}));

                assertThatThrownBy(() -> seg.read(0, 100))
                        .isInstanceOf(java.io.EOFException.class);
            }
        }

        @Test
        void write_with_empty_buffer_throws_illegal_argument_exception() throws IOException {
            Path file = tempDir.resolve("empty-write.log");
            try (AolFileChannel seg = AolFileChannel.createNew(file)) {
                assertThatThrownBy(() -> seg.write(ByteBuffer.allocate(0)))
                        .isInstanceOf(IllegalArgumentException.class);
            }
        }
    }

    // ======================================================================
    // Helpers
    // ======================================================================

    private static QueuedMessage msg(long id, String rk, Map<String, String> headers,
                                     byte[] payload, boolean redeliv, int attempts) {
        return new QueuedMessage(id, payload, headers, rk, 1_700_000_000_000L, redeliv, attempts);
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
}
