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

            long expiryTime = -1L;

            while (true) {
                int opCode = bis.read();
                if (opCode == -1) {
                    break;
                }

                switch (opCode) {
                    case 0xFA:
                        readStringEncoded();
                        readStringEncoded();
                        break;
                    case 0xFE:
                        readLength();
                        break;
                    case 0xFB:
                        readLength();
                        readLength();
                        break;
                    case 0xFD:
                        expiryTime = readExpirySeconds();
                        break;
                    case 0xFC:
                        expiryTime = readExpiryMillis();
                        break;
                    case 0x00:
                        byte[] key = readStringEncoded();
                        byte[] value = readStringEncoded();

                        // **代码优化**: 创建一个明确的 Long 对象，让逻辑更清晰
                        Long expiry = expiryTime != -1L ? Long.valueOf(expiryTime) : null;
                        dataStore.setString(new String(key, StandardCharsets.UTF_8), new ValueEntry(value, expiry));

                        expiryTime = -1L;
                        break;
                    case 0xFF:
                        if (bis.available() >= 8) {
                            bis.readNBytes(8);
                        }
                        return;
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            // 为了调试，保留详细的错误打印
            System.out.println("Exception in RdbParser.parse: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ... (其他方法保持不变) ...

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
        return seconds * 1000;
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
        if (type == 0) {
            return firstByte & 0x3F;
        } else if (type == 1) {
            int nextByte = bis.read();
            return ((firstByte & 0x3F) << 8) | nextByte;
        } else if (type == 2) {
            return bis.read() << 24 | bis.read() << 16 | bis.read() << 8 | bis.read();
        }
        return -1;
    }

    private byte[] readStringEncoded() throws IOException {
        int firstByte = bis.read();
        if (firstByte == -1) {
            throw new IOException("End of stream");
        }

        int type = (firstByte & 0xC0) >> 6;
        if (type == 3) {
            int encoding = firstByte & 0x3F;
            if (encoding == 0) {
                return String.valueOf(bis.read()).getBytes();
            } else if (encoding == 1) {
                int value = (bis.read() & 0xFF) | ((bis.read() & 0xFF) << 8);
                return String.valueOf(value).getBytes();
            } else if (encoding == 2) {
                long value = ((long)bis.read() & 0xFF) | (((long)bis.read() & 0xFF) << 8) | (((long)bis.read() & 0xFF) << 16) | (((long)bis.read() & 0xFF) << 24);
                return String.valueOf(value).getBytes();
            } else {
                throw new IOException("Unknown string encoding type: " + encoding);
            }
        } else {
            int length;
            if (type == 0) {
                length = firstByte & 0x3F;
            } else if (type == 1) {
                int nextByte = bis.read();
                length = ((firstByte & 0x3F) << 8) | nextByte;
            } else {
                length = bis.read() << 24 | bis.read() << 16 | bis.read() << 8 | bis.read();
            }
            return bis.readNBytes(length);
        }
    }
}
