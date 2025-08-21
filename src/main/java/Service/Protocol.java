package Service;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

class CommandResult {
    public final List<byte[]> parts;
    public final long bytesRead;

    public CommandResult(List<byte[]> parts, long bytesRead) {
        this.parts = parts;
        this.bytesRead = bytesRead;
    }
}

class CountingInputStream extends FilterInputStream {
    private long count = 0;
    protected CountingInputStream(InputStream in) { super(in); }
    @Override
    public int read() throws IOException {
        int result = super.read();
        if (result != -1) { count++; }
        return result;
    }
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int result = super.read(b, off, len);
        if (result != -1) { count += result; }
        return result;
    }
    public long getCount() { return count; }
    public void resetCount() { this.count = 0; }
}

public class Protocol {
    private final CountingInputStream is;

    public Protocol(InputStream is) {
        this.is = new CountingInputStream(is);
    }

    public CommandResult readCommandWithCount() throws IOException {
        is.resetCount();
        List<byte[]> commandParts = readCommandInternal();
        if (commandParts == null) { return null; }
        return new CommandResult(commandParts, is.getCount());
    }

    public List<byte[]> readCommand() throws IOException {
        return readCommandInternal();
    }

    private List<byte[]> readCommandInternal() throws IOException {
        int firstByte = is.read();
        if (firstByte == -1) { return null; }
        char type = (char) firstByte;
        if (type == '*') { return readArray(); }
        else { throw new IOException("Unsupported RESP type as top-level command: " + type); }
    }

    public String readSimpleString() throws IOException {
        int firstByte = is.read();
        if (firstByte == -1) { return null; }
        if ((char) firstByte != '+') { throw new IOException("Expected Simple String to start with '+', but got " + (char)firstByte); }
        return readLine();
    }

    private List<byte[]> readArray() throws IOException {
        int arrayLength = readInteger();
        if (arrayLength == -1) { return null; }
        List<byte[]> result = new ArrayList<>(arrayLength);
        for (int i = 0; i < arrayLength; i++) {
            result.add(readBulkString());
        }
        return result;
    }

    /**
     * **关键修复**: 重写此方法以确保完整读取数据。
     */
    private byte[] readBulkString() throws IOException {
        int firstByte = is.read();
        if (firstByte == -1) { return null; }

        char type = (char) firstByte;
        if (type != '$') { throw new IOException("Expected Bulk String, but got type: " + type); }

        int stringLength = readInteger();
        if (stringLength == -1) { return null; } // RESP Null Bulk String

        byte[] data = new byte[stringLength];
        int totalBytesRead = 0;
        // 必须在循环中读取，因为一次 is.read() 不保证能读完所有数据
        while (totalBytesRead < stringLength) {
            int bytesRead = is.read(data, totalBytesRead, stringLength - totalBytesRead);
            if (bytesRead == -1) {
                throw new IOException("Unexpected end of stream while reading bulk string data.");
            }
            totalBytesRead += bytesRead;
        }

        // 消费结尾的 CRLF (\r\n)
        if (is.read() != '\r' || is.read() != '\n') {
            throw new IOException("Expected CRLF after bulk string data.");
        }

        return data;
    }

    private int readInteger() throws IOException {
        String line = readLine();
        return Integer.parseInt(line);
    }

    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = is.read()) != '\r') {
            if (b == -1) { throw new IOException("Unexpected end of stream."); }
            sb.append((char) b);
        }
        if (is.read() != '\n') { throw new IOException("Expected LF after CR in line ending."); }
        return sb.toString();
    }
}
