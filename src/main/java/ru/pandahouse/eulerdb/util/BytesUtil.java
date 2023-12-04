package ru.pandahouse.eulerdb.util;

public class BytesUtil {

    public static byte[] to4ByteArray(int value) {
        return new byte[] {
            (byte) (value >> 24),
            (byte) (value >> 16),
            (byte) (value >> 8),
            (byte) value
        };
    }

    public static int from4ByteArray(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) |
               ((bytes[1] & 0xFF) << 16) |
               ((bytes[2] & 0xFF) << 8 ) |
               (bytes[3] & 0xFF);
    }

}
