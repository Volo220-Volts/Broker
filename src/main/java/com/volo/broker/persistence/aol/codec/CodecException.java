package com.volo.broker.persistence.aol.codec;

public class CodecException extends RuntimeException {

    public CodecException(String message) {
        super(message);
    }

    public CodecException(String message, Throwable cause) {
        super(message, cause);
    }


    public static final class ChecksumMismatchException extends CodecException {

        private final long queueOffset;
        private final int storedCrc;
        private final int computedCrc;

        public ChecksumMismatchException(long queueOffset, int storedCrc, int computedCrc) {
            super("CRC32C mismatch at queueOffset=%d: stored=0x%08X computed=0x%08X"
                    .formatted(queueOffset, Integer.toUnsignedLong(storedCrc),
                            Integer.toUnsignedLong(computedCrc)));
            this.queueOffset = queueOffset;
            this.storedCrc = storedCrc;
            this.computedCrc = computedCrc;
        }

        public long queueOffset() { return queueOffset; }
        public int storedCrc()    { return storedCrc; }
        public int computedCrc()  { return computedCrc; }
    }

    public static final class UnsupportedVersionException extends CodecException {

        private final byte version;

        public UnsupportedVersionException(byte version) {
            super("Unsupported AOL record version: " + Byte.toUnsignedInt(version)
                    + ". Supported: " + DataRecordCodec.CURRENT_VERSION);
            this.version = version;
        }

        public byte version() { return version; }
    }
}
