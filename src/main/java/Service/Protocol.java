// 文件路径: src/main/java/Service/Protocol.java
package Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Protocol {
    private final InputStream inputStream;
    private long bytesRead = 0L;

    public Protocol(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    // 新增：重置计数器的方法
    public void resetBytesRead() {
        this.bytesRead = 0;
    }

    public long getBytesRead() {
        return this.bytesRead;
    }

    public List<byte[]> readCommand() throws IOException {
        int firstByte = readByte(); // 修改：使用封装的方法
        if (firstByte == -1) {
            return null;
        }
        if ((char) firstByte == '*') {
            return parseArray();
        }
        throw new IOException("Unsupported command format. Expected an Array ('*').");
    }

    public String readSimpleString() throws IOException {
        int firstByte = readByte();
        if (firstByte == -1) {
            return null;
        }
        if ((char) firstByte == '+') {
            return readLine();
        }
        throw new IOException("Expected a Simple String ('+').");
    }

    public byte[] readRdbFile() throws IOException {
        int firstByte = readByte();
        if (firstByte == -1) {
            return null;
        }
        if ((char) firstByte != '$') {
            throw new IOException("Expected a Bulk String for RDB file.");
        }

        int rdbFileLength = Integer.parseInt(readLine());
        if (rdbFileLength < 0) {
            return null;
        }

        byte[] rdbData = new byte[rdbFileLength];
        readFully(rdbData);
        return rdbData;
    }

    private List<byte[]> parseArray() throws IOException {
        int arraySize = Integer.parseInt(readLine());
        if (arraySize < 0) {
            return null;
        }
        List<byte[]> commandParts = new ArrayList<>(arraySize);
        for (int i = 0; i < arraySize; i++) {
            int firstByte = readByte();
            if (firstByte != '$') {
                throw new IOException("Unsupported element type in Array. Expected Bulk String ('$').");
            }
            commandParts.add(parseBulkString());
        }
        return commandParts;
    }

    private byte[] parseBulkString() throws IOException {
        int stringLength = Integer.parseInt(readLine());
        if (stringLength < 0) {
            return null;
        }
        byte[] data = new byte[stringLength];
        readFully(data);

        if (readByte() != '\r' || readByte() != '\n') {
            throw new IOException("Expected CRLF after Bulk String data.");
        }
        return data;
    }

    private String readLine() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int b;
        while ((b = readByte()) != '\r') {
            if (b == -1) {
                throw new IOException("Unexpected end of stream.");
            }
            baos.write(b);
        }
        if (readByte() != '\n') {
            throw new IOException("Expected LF after CR.");
        }
        return baos.toString(StandardCharsets.UTF_8);
    }

    private int readByte() throws IOException {
        int b = inputStream.read();
        if (b != -1) {
            bytesRead++;
        }
        return b;
    }

    private void readFully(byte[] buffer) throws IOException {
        int bytesToRead = buffer.length;
        int totalBytesRead = 0;
        while (totalBytesRead < bytesToRead) {
            int read = inputStream.read(buffer, totalBytesRead, bytesToRead - totalBytesRead);
            if (read == -1) {
                throw new IOException("Unexpected end of stream.");
            }
            totalBytesRead += read;
            this.bytesRead += read;
        }
    }
}
