package Service;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * 一个辅助类，用于包装 RESP 命令的解析结果。
 * 它同时包含解析出的命令部分和读取该命令所消耗的总字节数。
 */
class CommandResult {
    public final List<byte[]> parts;
    public final long bytesRead;

    public CommandResult(List<byte[]> parts, long bytesRead) {
        this.parts = parts;
        this.bytesRead = bytesRead;
    }
}

/**
 * 一个自定义的 InputStream，它包装了另一个 InputStream，
 * 并自动计算从流中读取的总字节数。
 * * **重要**: 这个类继承自 FilterInputStream，用于“过滤”或“包装”另一个流，
 * 而不是从头创建一个新的流。
 */
class CountingInputStream extends FilterInputStream {
    private long count = 0;

    /**
     * 构造函数。
     * @param in 需要被包装和计数的原始输入流 (例如，来自网络套接字)。
     */
    protected CountingInputStream(InputStream in) {
        // **正确实现**: 调用父类 FilterInputStream 的构造函数，
        // 将原始输入流传递进去。
        super(in);
    }

    @Override
    public int read() throws IOException {
        int result = super.read();
        if (result != -1) {
            count++;
        }
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int result = super.read(b, off, len);
        if (result != -1) {
            count += result;
        }
        return result;
    }

    public long getCount() {
        return count;
    }

    public void resetCount() {
        this.count = 0;
    }
}

/**
 * RESP (Redis Serialization Protocol) 解析器。
 */
public class Protocol {
    private final CountingInputStream is;

    public Protocol(InputStream is) {
        // 将原始的 InputStream 包装成我们带计数功能的 CountingInputStream
        this.is = new CountingInputStream(is);
    }

    /**
     * 从流中读取并解析一个完整的 Redis 命令。
     * @return CommandResult 对象，包含命令和读取的字节数；如果流结束则返回 null。
     * @throws IOException 如果发生 I/O 错误或协议格式错误。
     */
    public CommandResult readCommandWithCount() throws IOException {
        is.resetCount(); // 为每个新命令重置字节计数器

        List<byte[]> commandParts = readCommandInternal();
        if (commandParts == null) {
            return null;
        }

        return new CommandResult(commandParts, is.getCount());
    }

    /**
     * 兼容旧的 readCommand 方法，用于不需要计数的场景。
     */
    public List<byte[]> readCommand() throws IOException {
        return readCommandInternal();
    }

    private List<byte[]> readCommandInternal() throws IOException {
        int firstByte = is.read();
        if (firstByte == -1) {
            return null; // 到达流的末尾
        }

        char type = (char) firstByte;
        if (type == '*') {
            return readArray();
        } else {
            throw new IOException("Unsupported RESP type as top-level command: " + type);
        }
    }

    public String readSimpleString() throws IOException {
        int firstByte = is.read();
        if (firstByte == -1) {
            return null;
        }
        if ((char) firstByte != '+') {
            throw new IOException("Expected Simple String to start with '+', but got " + (char)firstByte);
        }
        return readLine();
    }

    private List<byte[]> readArray() throws IOException {
        int arrayLength = readInteger();
        if (arrayLength == -1) {
            return null;
        }
        List<byte[]> result = new ArrayList<>(arrayLength);
        for (int i = 0; i < arrayLength; i++) {
            result.add(readBulkString());
        }
        return result;
    }

    private byte[] readBulkString() throws IOException {
        int firstByte = is.read();
        if (firstByte == -1) {
            return null;
        }

        char type = (char) firstByte;
        if (type != '$') {
            throw new IOException("Expected Bulk String, but got type: " + type);
        }

        int stringLength = readInteger();
        if (stringLength == -1) {
            return null;
        }

        byte[] data = new byte[stringLength];
        int bytesRead = 0;
        while (bytesRead < stringLength) {
            int read = is.read(data, bytesRead, stringLength - bytesRead);
            if (read == -1) {
                throw new IOException("Unexpected end of stream while reading bulk string data.");
            }
            bytesRead += read;
        }

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
            if (b == -1) {
                throw new IOException("Unexpected end of stream.");
            }
            sb.append((char) b);
        }
        if (is.read() != '\n') {
            throw new IOException("Expected LF after CR in line ending.");
        }
        return sb.toString();
    }
}
