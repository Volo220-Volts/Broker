package com.volo.broker.persistence.aol.io;

import com.volo.broker.persistence.aol.codec.CodecException;
import com.volo.broker.persistence.aol.codec.DataRecordCodec;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Scans the tail of an active data segment to find the last complete,
 * CRC-valid record and reports the truncation point.
 *
 */
public final class SegmentTailValidator {

    /** Byte size of the {@code recordLength} field at the start of every record. */
    private static final int RECORD_LENGTH_FIELD_SIZE = Integer.BYTES; // 4

    /** Minimum plausible record length: fixed body (39) + crc (4). */
    private static final int MINIMUM_RECORD_LENGTH =
            DataRecordCodec.FIXED_BODY_SIZE + Integer.BYTES; // 43

    private final DataRecordCodec codec;

    public SegmentTailValidator(DataRecordCodec codec) {
        if (codec == null) throw new NullPointerException("codec must not be null");
        this.codec = codec;
    }

    // ------------------------------------------------------------------ public API

    /**
     * Result of a tail-validation scan.
     *
     * @param lastValidPosition byte offset immediately after the last valid, complete record.
     * @param recordsFound      total number of valid records found during the scan
     * @param truncationNeeded  {@code true} if the file contains bytes beyond
     *                          {@code lastValidPosition} (i.e. a partial record was detected)
     */
    public record ValidationResult(
            long lastValidPosition,
            int  recordsFound,
            boolean truncationNeeded
    ) {}

    /**
     * Scans {@code file} from the beginning, walking through records one-by-one.
     * Returns the position of the last byte after the last fully valid record.
     *
     * @param file the segment file to validate;
     * @return validation result describing the truncation point and record count
     * @throws IOException if reading the file fails for reasons other than a corrupt record
     */
    public ValidationResult validate(AolFile file) throws IOException {
        long fileSize          = file.size();
        long scanPosition      = 0;
        long lastValidPosition = 0;
        int  recordsFound      = 0;

        while (scanPosition + RECORD_LENGTH_FIELD_SIZE <= fileSize) {

            ByteBuffer lenBuf = file.read(scanPosition, RECORD_LENGTH_FIELD_SIZE);
            int recordLength  = lenBuf.getInt();

            if (recordLength < MINIMUM_RECORD_LENGTH) {
                break;
            }

            long recordEnd = scanPosition + RECORD_LENGTH_FIELD_SIZE + recordLength;
            if (recordEnd > fileSize) {
                // Record body was not fully written
                break;
            }

            int totalRecordSize = RECORD_LENGTH_FIELD_SIZE + recordLength;
            ByteBuffer recordBuf = file.read(scanPosition, totalRecordSize);

            try {
                codec.decode(recordBuf);
                lastValidPosition = recordEnd;
                recordsFound++;
                scanPosition = recordEnd;
            } catch (CodecException e) {
                // TODO: Corrupt record (bad CRC, unsupported version, etc.) → stop here
                break;
            }
        }

        boolean truncationNeeded = lastValidPosition < fileSize;
        return new ValidationResult(lastValidPosition, recordsFound, truncationNeeded);
    }
}
