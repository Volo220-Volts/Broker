package com.volo.broker.persistence.aol.codec;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;


public final class Crc32cUtil {

    private Crc32cUtil() {}

    public static int compute(byte[] buf, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(buf, offset, length);
        return (int) crc.getValue();
    }


    public static int compute(ByteBuffer buf) {
        CRC32C crc = new CRC32C();
        crc.update(buf.duplicate());
        return (int) crc.getValue();
    }
}
