package util;

import Storage.DataStore;
import Storage.ValueEntry;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author Achilles
 * 负责解析 RDB 文件并加载数据到 DataStore
 */
public class RdbParser {
    private final File rdbFile;
    private final DataStore dataStore;
    private BufferedInputStream bis;

    public RdbParser(File rdbFile, DataStore dataStore) {
        this.rdbFile = rdbFile;
        this.dataStore = dataStore;
    }

    public void parse() throws IOException {
        try (FileInputStream fis = new FileInputStream(rdbFile)) {
            this.bis = new BufferedInputStream(fis);

            verifyHeader();

            long expiryTime = -1;

            while (true) {
                int opCode = bis.read();
                if (opCode == -1) {
                    break;
                }

                switch (opCode) {
                    case 0xFA: // AUX field
                        readStringEncoded(); // Read and discard key
                        readStringEncoded(); // Read and discard value
                        break;
                    case 0xFE: // SELECTDB
                        readLength(); // Read and discard db number
                        break;
                    case 0xFB: // RESIZEDB
                        readLength(); // Read and discard hash table size
                        readLength(); // Read and discard expire hash table size
                        break;
                    case 0xFD: // EXPIRETIME (seconds)
                        expiryTime = readExpirySeconds();
                        break;
                    case 0xFC: // EXPIRETIME (milliseconds)
                        expiryTime = readExpiryMillis();
                        break;
                    case 0x00: // Value type: String
                        byte[] key = readStringEncoded();
                        byte[] value = readStringEncoded();
                        dataStore.setString(new String(key, StandardCharsets.UTF_8), new ValueEntry(value, expiryTime != -1 ? expiryTime : null));
                        expiryTime = -1; // Reset expiry time for the next key
                        break;
                    case 0xFF: // EOF
                        // Before returning, check for checksum
                        bis.readNBytes(8); // Read and discard 8-byte checksum
                        return; // End of file
                    default:
                        System.out.println("Ignoring unknown opcode: " + opCode);
                        break;
                }
            }
        }
    }

    private void verifyHeader() throws IOException {
        byte[] header = bis.readNBytes(9);
        String magic = new String(header, 0, 5, StandardCharsets.UTF_8);
        if (!"REDIS".equals(magic)) {
            throw new IOException("Invalid RDB file format: Magic string is incorrect.");
        }
    }

    private long readExpirySeconds() throws IOException {
        byte[] bytes = bis.readNBytes(4);
        long seconds = ((long) (bytes[3] & 0xFF) << 24) |
                ((long) (bytes[2] & 0xFF) << 16) |
                ((long) (bytes[1] & 0xFF) << 8) |
                ((long) (bytes[0] & 0xFF));
        return seconds * 1000; // Convert to milliseconds
    }

    private long readExpiryMillis() throws IOException {
        byte[] bytes = bis.readNBytes(8);
        return ((long) (bytes[7] & 0xFF) << 56) |
                ((long) (bytes[6] & 0xFF) << 48) |
                ((long) (bytes[5] & 0xFF) << 40) |
                ((long) (bytes[4] & 0xFF) << 32) |
                ((long) (bytes[3] & 0xFF) << 24) |
                ((long) (bytes[2] & 0xFF) << 16) |
                ((long) (bytes[1] & 0xFF) << 8) |
                ((long) (bytes[0] & 0xFF));
    }

    private int readLength() throws IOException {
        int firstByte = bis.read();
        if (firstByte == -1) {
            throw new IOException("End of stream");
        }
        int type = (firstByte & 0xC0) >> 6;
        if (type == 0) { // 6-bit length
            return firstByte & 0x3F;
        } else if (type == 1) { // 14-bit length
            int nextByte = bis.read();
            return ((firstByte & 0x3F) << 8) | nextByte;
        } else if (type == 2) { // 32-bit length
            return bis.read() << 24 | bis.read() << 16 | bis.read() << 8 | bis.read();
        }
        return -1; // Should not happen for length
    }

    /**
     * **关键修复**: 这个方法现在可以处理普通字符串和整数编码的字符串。
     */
    private byte[] readStringEncoded() throws IOException {
        int firstByte = bis.read();
        if (firstByte == -1) {
            throw new IOException("End of stream");
        }

        int type = (firstByte & 0xC0) >> 6;
        if (type == 3) { // Special encoded format
            int encoding = firstByte & 0x3F;
            if (encoding == 0) { // 8-bit integer
                return String.valueOf(bis.read()).getBytes();
            } else if (encoding == 1) { // 16-bit integer
                int value = (bis.read() & 0xFF) | ((bis.read() & 0xFF) << 8);
                return String.valueOf(value).getBytes();
            } else if (encoding == 2) { // 32-bit integer
                long value = ((long)bis.read() & 0xFF) | (((long)bis.read() & 0xFF) << 8) | (((long)bis.read() & 0xFF) << 16) | (((long)bis.read() & 0xFF) << 24);
                return String.valueOf(value).getBytes();
            } else {
                throw new IOException("Unknown string encoding type: " + encoding);
            }
        } else { // Length-prefixed string
            int length;
            if (type == 0) {
                length = firstByte & 0x3F;
            } else if (type == 1) {
                int nextByte = bis.read();
                length = ((firstByte & 0x3F) << 8) | nextByte;
            } else { // type == 2
                length = bis.read() << 24 | bis.read() << 16 | bis.read() << 8 | bis.read();
            }
            return bis.readNBytes(length);
        }
    }
}
