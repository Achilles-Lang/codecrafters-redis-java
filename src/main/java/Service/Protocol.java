package Service;

import Storage.CommandResult;
import util.CountingInputStream;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
/**
 * RESP 请求解析器
 * */
public class Protocol {
    private final CountingInputStream is;

    public Protocol(InputStream is) throws FileNotFoundException {
        // 将原始的 InputStream 包装成我们带计数功能的 CountingInputStream
        this.is = new CountingInputStream(is);
    }

    /**
     * 从流中读取并解析一个完整的 Redis 命令。
     * @return CommandResult 对象，包含命令和读取的字节数；如果流结束则返回 null。
     * @throws IOException 如果发生 I/O 错误或协议格式错误。
     */
    public CommandResult readCommand() throws IOException {
        is.resetCount(); // 为每个新命令重置字节计数器
        int firstByte = is.read();
        if (firstByte == -1) {
            return null; // 到达流的末尾
        }

        char type = (char) firstByte;
        if (type == '*') {
            // Redis 命令总是以 RESP Array 的形式出现
            return new CommandResult(readArray(), (int) is.getCount());
        } else {
            // 为了简化，这个解析器只处理作为顶层数据类型的数组。
            throw new IOException("Unsupported RESP type as top-level command: " + type);
        }
    }

    /**
     * 读取一个 RESP Simple String (以 '+' 开头)。
     * @return 解析出的字符串。
     * @throws IOException 如果发生 I/O 错误或协议格式错误。
     */
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

    /**
     * 解析一个 RESP Array。
     */
    private List<byte[]> readArray() throws IOException {
        int arrayLength = readInteger();
        if (arrayLength == -1) {
            return null; // RESP Null Array
        }
        List<byte[]> result = new ArrayList<>(arrayLength);
        for (int i = 0; i < arrayLength; i++) {
            result.add(readBulkString());
        }
        return result;
    }

    /**
     * 解析一个 RESP Bulk String (以 '$' 开头)。
     */
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
            return null; // RESP Null Bulk String
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

        // 读取并消费末尾的 CRLF (\r\n)
        if (is.read() != '\r' || is.read() != '\n') {
            throw new IOException("Expected CRLF after bulk string data.");
        }

        return data;
    }

    /**
     * 读取一个以 CRLF 结尾的行，并将其内容解析为整数。
     */
    private int readInteger() throws IOException {
        String line = readLine();
        return Integer.parseInt(line);
    }

    /**
     * 从流中读取一行文本（直到 CRLF）。
     */
    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = is.read()) != '\r') {
            if (b == -1) {
                throw new IOException("Unexpected end of stream.");
            }
            sb.append((char) b);
        }
        // 消费 '\n'
        if (is.read() != '\n') {
            throw new IOException("Expected LF after CR in line ending.");
        }
        return sb.toString();
    }

}
