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

    public void readRdbFile() throws IOException {
        int firstByte = is.read();
        if (firstByte != '$') {
            throw new IOException("Expected RDB file to start with '$', but got: " + (char)firstByte);
        }
        // '$' 已被消费，现在读取 Bulk String 的剩余部分
        readBulkStringPayload();
    }

    public CommandResult readCommandWithCount() throws IOException {
        is.resetCount();
        Object parsed = parseOne();
        if (parsed instanceof List) {
            // RESP 命令总是以数组形式出现，我们需要将 List<Object> 转换为 List<byte[]>
            List<?> objectList = (List<?>) parsed;
            List<byte[]> commandParts = new ArrayList<>();
            for (Object item : objectList) {
                if (item instanceof byte[]) {
                    commandParts.add((byte[]) item);
                } else {
                    throw new IOException("Command array contained non-bulk string element.");
                }
            }
            return new CommandResult(commandParts, is.getCount());
        }
        throw new IOException("Expected command to be a RESP Array, but got: " + (parsed != null ? parsed.getClass().getSimpleName() : "null"));
    }

    public List<byte[]> readCommand() throws IOException {
        CommandResult result = readCommandWithCount();
        return result.parts;
    }

    /**
     * **核心修复**: 这是一个通用的解析方法，能处理任何 RESP 类型。
     */
    private Object parseOne() throws IOException {
        int firstByte = is.read();
        if (firstByte == -1) { return null; }
        char type = (char) firstByte;
        switch (type) {
            case '+': return readLine();
            case '$': return readBulkStringPayload();
            case '*': return readArrayPayload();
            default: throw new IOException("Unsupported RESP type: " + type);
        }
    }

    private List<Object> readArrayPayload() throws IOException {
        int arrayLength = readInteger();
        if (arrayLength == -1) { return null; }
        List<Object> result = new ArrayList<>(arrayLength);
        for (int i = 0; i < arrayLength; i++) {
            // 递归调用 parseOne 来解析数组中的每一个元素
            result.add(parseOne());
        }
        return result;
    }

    private byte[] readBulkStringPayload() throws IOException {
        int stringLength = readInteger();
        if (stringLength == -1) { return null; }

        byte[] data = new byte[stringLength];
        int totalBytesRead = 0;
        while (totalBytesRead < stringLength) {
            int bytesRead = is.read(data, totalBytesRead, stringLength - totalBytesRead);
            if (bytesRead == -1) {
                throw new IOException("Unexpected end of stream while reading bulk string data.");
            }
            totalBytesRead += bytesRead;
        }

        if (is.read() != '\r' || is.read() != '\n') {
            throw new IOException("Expected CRLF after bulk string data.");
        }
        return data;
    }

    private int readInteger() throws IOException {
        return Integer.parseInt(readLine());
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
