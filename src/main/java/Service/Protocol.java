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
        int firstByte = readByte(); // 修改：使用封装的方法
        if (firstByte == -1) {
            return null;
        }
        if ((char) firstByte == '+') {
            return readLine();
        }
        // 如果不是 '+'，则抛出异常或返回 null，具体取决于协议要求
        throw new IOException("Expected a Simple String ('+').");
    }

    public byte[] readRdbFile() throws IOException {
        int firstByte = readByte(); // 修改：使用封装的方法
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
        readFully(rdbData); // 修改：使用封装的方法
        return rdbData;
    }

    private List<byte[]> parseArray() throws IOException {
        int arraySize = Integer.parseInt(readLine());
        if (arraySize < 0) {
            return null;
        }
        List<byte[]> commandParts = new ArrayList<>(arraySize);
        for (int i = 0; i < arraySize; i++) {
            int firstByte = readByte(); // 修改：使用封装的方法
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
        readFully(data); // 修改：使用封装的方法

        // 读取并验证末尾的 CRLF
        if (readByte() != '\r' || readByte() != '\n') {
            throw new IOException("Expected CRLF after Bulk String data.");
        }
        return data;
    }

    private String readLine() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int b;
        while ((b = readByte()) != '\r') { // 修改：使用封装的方法
            if (b == -1) {
                throw new IOException("Unexpected end of stream.");
            }
            baos.write(b);
        }
        if (readByte() != '\n') { // 修改：使用封装的方法
            throw new IOException("Expected LF after CR.");
        }
        return baos.toString(StandardCharsets.UTF_8);
    }

    // 新增：封装的单字节读取方法，用于计数
    private int readByte() throws IOException {
        int b = inputStream.read();
        if (b != -1) {
            bytesRead++;
        }
        return b;
    }

    // 新增：封装的多字节读取方法，用于计数
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