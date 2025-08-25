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

    public Protocol(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public List<byte[]> readCommand() throws IOException {
        int firstByte = inputStream.read();
        if (firstByte == -1) {
            return null;
        }
        if ((char) firstByte == '*') {
            return parseArray();
        }
        throw new IOException("Unsupported command format. Expected an Array ('*').");
    }

    public String readSimpleString() throws IOException {
        if (inputStream.read() == '+') {
            return readLine();
        }
        return null;
    }

    public byte[] readRdbFile() throws IOException {
        int firstByte = inputStream.read();
        if (firstByte == -1) {
            return null;
        }
        if ((char) firstByte != '$') {
            throw new IOException("Expected a Bulk String for RDB file.");
        }

        // 读取 Bulk String 的长度
        int rdbFileLength = Integer.parseInt(readLine());
        if (rdbFileLength < 0) {
            // null bulk string
            return null;
        }

        // 根据长度读取 RDB 的二进制数据，不读取任何其他字节
        byte[] rdbData = new byte[rdbFileLength];
        int bytesRead = 0;
        while (bytesRead < rdbFileLength) {
            int read = inputStream.read(rdbData, bytesRead, rdbFileLength - bytesRead);
            if (read == -1) {
                throw new IOException("Unexpected end of stream while reading RDB file.");
            }
            bytesRead += read;
        }

        return rdbData;
    }

    private List<byte[]> parseArray() throws IOException {
        int arraySize = Integer.parseInt(readLine());
        if (arraySize < 0) {
            return null;
        }
        List<byte[]> commandParts = new ArrayList<>(arraySize);
        for (int i = 0; i < arraySize; i++) {
            if (inputStream.read() == '$') {
                commandParts.add(parseBulkString());
            } else {
                throw new IOException("Unsupported element type in Array. Expected Bulk String ('$').");
            }
        }
        return commandParts;
    }

    private byte[] parseBulkString() throws IOException {
        int stringLength = Integer.parseInt(readLine());
        if (stringLength < 0) {
            return null;
        }
        byte[] data = new byte[stringLength];
        int bytesRead = 0;
        while (bytesRead < stringLength) {
            int read = inputStream.read(data, bytesRead, stringLength - bytesRead);
            if (read == -1) {
                throw new IOException("Unexpected end of stream while reading Bulk String data.");
            }
            bytesRead += read;
        }

        int cr=inputStream.read();
        int lf=inputStream.read();
        if(cr!='\r' || lf!='\n'){
            throw new IOException("Expected CRLF after Bulk String data. Got: " + cr + ", " + lf);
        }
        return data;
    }

    private String readLine() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int b;
        while ((b = inputStream.read()) != '\r') {
            if (b == -1) {
                throw new IOException("Unexpected end of stream.");
            }
            baos.write(b);
        }
        if (inputStream.read() != '\n') {
            throw new IOException("Expected LF after CR.");
        }
        return baos.toString(StandardCharsets.UTF_8);
    }
}