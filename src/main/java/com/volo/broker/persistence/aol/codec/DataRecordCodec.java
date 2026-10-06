package com.volo.broker.persistence.aol.codec;

import com.volo.broker.broker.message.QueuedMessage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Encodes and decodes {@link QueuedMessage} instances to and from the binary
 * Append-Only Log (AOL) data record format.
 */
public final class DataRecordCodec {

    public static final byte CURRENT_VERSION = 1;

    private static final byte FLAGS_NONE = 0;

    public static final int FIXED_BODY_SIZE = 39;

    private static final int RECORD_LENGTH_FIELD_SIZE = Integer.BYTES; // 4
    private static final int CRC_SIZE                 = Integer.BYTES; // 4

    private static final int MAX_ROUTING_KEY_BYTES = 0xFFFF; // 65 535
    private static final int MAX_HEADER_KEY_BYTES  = 0xFFFF;
    private static final int MAX_HEADER_VAL_BYTES  = 0xFFFF;
    private static final int MAX_HEADERS_COUNT     = 0xFFFF;

    // ------------------------------------------------------------------ encode

    /**
     * Encodes a {@link QueuedMessage} into a complete AOL data record.
     *
     * @param message     the message to encode;
     * @param queueOffset offset of this message within its queue
     * @return a read-only {@link ByteBuffer} positioned at 0, ready to be written
     *         to a {@link java.nio.channels.FileChannel}
     * @throws NullPointerException  if {@code message} is {@code null}
     * @throws CodecException        if any string field exceeds its size limit
     */
    public ByteBuffer encode(QueuedMessage message, long queueOffset) {
        if (message == null) {
            throw new NullPointerException("message must not be null");
        }

        byte[] routingKeyBytes = toUtf8(message.getRoutingKey());
        EncodedHeaders encodedHeaders = encodeHeaders(message.getHeaders());
        byte[] payload = message.getPayload();

        validateLengths(routingKeyBytes, message.getHeaders(), encodedHeaders);

        int bodySize = FIXED_BODY_SIZE
                + routingKeyBytes.length
                + encodedHeaders.totalBytes()
                + payload.length
                + CRC_SIZE;

        ByteBuffer buf = ByteBuffer.allocate(RECORD_LENGTH_FIELD_SIZE + bodySize);

        buf.putInt(bodySize);

        int crcStart = buf.position();

        buf.put(CURRENT_VERSION);
        buf.put(FLAGS_NONE);
        buf.putLong(queueOffset);
        buf.putLong(message.getMessageId());
        buf.putLong(message.getCreatedAt());
        buf.put(message.isRedelivered() ? (byte) 1 : (byte) 0);
        buf.putInt(message.getDeliveryAttempt());
        buf.putShort((short) routingKeyBytes.length);
        buf.putShort((short) message.getHeaders().size());
        buf.putInt(payload.length);

        buf.put(routingKeyBytes);

        for (EncodedHeaders.Entry entry : encodedHeaders.entries()) {
            buf.putShort((short) entry.keyBytes().length);
            buf.put(entry.keyBytes());
            buf.putShort((short) entry.valBytes().length);
            buf.put(entry.valBytes());
        }

        buf.put(payload);

        int crcEnd = buf.position();
        buf.flip();
        ByteBuffer crcRegion = buf.slice(crcStart, crcEnd - crcStart);
        int crc = Crc32cUtil.compute(crcRegion);

        buf.limit(buf.capacity());
        buf.position(crcEnd);
        buf.putInt(crc);

        buf.flip();
        return buf.asReadOnlyBuffer();
    }

    // ------------------------------------------------------------------ decode

    /**
     * Decodes a single AOL data record from {@code buf}.
     *
     * @param buf source buffer; must be a heap-backed {@link ByteBuffer}
     * @return the decoded record
     * @throws CodecException.ChecksumMismatchException   if CRC32C does not match
     * @throws CodecException.UnsupportedVersionException if the record version is unknown
     * @throws CodecException                             for any other structural error
     */
    public DecodedRecord decode(ByteBuffer buf) {
        int recordStart = buf.position();

        int recordLength = buf.getInt();
        validateRecordLength(recordLength, recordStart);

        int crcStart = buf.position();

        byte version = buf.get();
        if (version != CURRENT_VERSION) {
            throw new CodecException.UnsupportedVersionException(version);
        }
        buf.get();

        long queueOffset      = buf.getLong();
        long messageId        = buf.getLong();
        long createdAt        = buf.getLong();
        boolean redelivered   = buf.get() == 1;
        int deliveryAttempts  = buf.getInt();
        int routingKeyLength  = Short.toUnsignedInt(buf.getShort());
        int headersCount      = Short.toUnsignedInt(buf.getShort());
        int payloadLength     = buf.getInt();

        byte[] routingKeyBytes = readBytes(buf, routingKeyLength, "routingKey");
        String routingKey = new String(routingKeyBytes, StandardCharsets.UTF_8);

        Map<String, String> headers = new HashMap<>(headersCount * 2);
        for (int i = 0; i < headersCount; i++) {
            String key = readUtf8(buf, "header key");
            String val = readUtf8(buf, "header value");
            headers.put(key, val);
        }

        byte[] payload = readBytes(buf, payloadLength, "payload");

        int crcEnd = buf.position();
        int storedCrc = buf.getInt();

        ByteBuffer crcRegion = buf.slice(crcStart, crcEnd - crcStart);
        int computedCrc = Crc32cUtil.compute(crcRegion);

        if (storedCrc != computedCrc) {
            throw new CodecException.ChecksumMismatchException(queueOffset, storedCrc, computedCrc);
        }

        QueuedMessage message = new QueuedMessage(
                messageId, payload, headers, routingKey,
                createdAt, redelivered, deliveryAttempts);

        return new DecodedRecord(queueOffset, message);
    }

    // ------------------------------------------------------------------ size estimation

    /**
     * Computes the total number of bytes this message will occupy on disk,
     * including the {@code recordLength} field and the {@code crc32c} footer.
     */
    public int computeEncodedSize(QueuedMessage message) {
        int headersBytes = message.getHeaders().entrySet().stream()
                .mapToInt(e -> Integer.BYTES
                             + toUtf8(e.getKey()).length
                             + toUtf8(e.getValue()).length)
                .sum();

        return RECORD_LENGTH_FIELD_SIZE
                + FIXED_BODY_SIZE
                + toUtf8(message.getRoutingKey()).length
                + headersBytes
                + message.getPayload().length
                + CRC_SIZE;
    }


    private static byte[] toUtf8(String s) {
        return (s == null ? "" : s).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] readBytes(ByteBuffer buf, int length, String fieldName) {
        if (length < 0 || length > buf.remaining()) {
            throw new CodecException(
                    "Invalid length %d for field '%s'; remaining bytes: %d"
                            .formatted(length, fieldName, buf.remaining()));
        }
        byte[] bytes = new byte[length];
        buf.get(bytes);
        return bytes;
    }

    private static String readUtf8(ByteBuffer buf, String fieldName) {
        int length = Short.toUnsignedInt(buf.getShort());
        return new String(readBytes(buf, length, fieldName), StandardCharsets.UTF_8);
    }

    private static void validateRecordLength(int recordLength, int recordStartPos) {
        int minimumBody = FIXED_BODY_SIZE + CRC_SIZE; // no variable fields
        if (recordLength < minimumBody) {
            throw new CodecException(
                    "recordLength=%d at byte position %d is smaller than minimum %d"
                            .formatted(recordLength, recordStartPos, minimumBody));
        }
    }

    private static void validateLengths(byte[] routingKeyBytes,
                                        Map<String, String> headers,
                                        EncodedHeaders encodedHeaders) {
        if (routingKeyBytes.length > MAX_ROUTING_KEY_BYTES) {
            throw new CodecException(
                    "Routing key exceeds maximum of %d bytes (got %d)"
                            .formatted(MAX_ROUTING_KEY_BYTES, routingKeyBytes.length));
        }
        if (headers.size() > MAX_HEADERS_COUNT) {
            throw new CodecException(
                    "Headers count %d exceeds maximum of %d"
                            .formatted(headers.size(), MAX_HEADERS_COUNT));
        }
        for (EncodedHeaders.Entry entry : encodedHeaders.entries()) {
            if (entry.keyBytes().length > MAX_HEADER_KEY_BYTES) {
                throw new CodecException(
                        "Header key length %d exceeds maximum of %d"
                                .formatted(entry.keyBytes().length, MAX_HEADER_KEY_BYTES));
            }
            if (entry.valBytes().length > MAX_HEADER_VAL_BYTES) {
                throw new CodecException(
                        "Header value length %d exceeds maximum of %d"
                                .formatted(entry.valBytes().length, MAX_HEADER_VAL_BYTES));
            }
        }
    }


    private record EncodedHeaders(java.util.List<Entry> entries) {

        record Entry(byte[] keyBytes, byte[] valBytes) {}

        int totalBytes() {
            return entries.stream()
                    .mapToInt(e -> 2 + e.keyBytes().length + 2 + e.valBytes().length)
                    .sum();
        }
    }

    private static EncodedHeaders encodeHeaders(Map<String, String> headers) {
        var entries = new java.util.ArrayList<EncodedHeaders.Entry>(headers.size());
        for (Map.Entry<String, String> h : headers.entrySet()) {
            entries.add(new EncodedHeaders.Entry(toUtf8(h.getKey()), toUtf8(h.getValue())));
        }
        return new EncodedHeaders(entries);
    }
}
