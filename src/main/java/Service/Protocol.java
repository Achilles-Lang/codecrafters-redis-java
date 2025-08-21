package Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
/**
 * RESP 请求解析器
 * */
public class Protocol {
    private final InputStream inputStream;

    public Protocol(InputStream inputStream) {
        this.inputStream = inputStream;
    }
    /*
    * 读取并解析一个完整的Redis命令
    * 在这个版本中，我们假设所有命令都以数组形式开始
    * @return 解析后的命令及参数列表，以字节数组列表形式返回。如果流关闭则返回 null。
    * */
    public List<byte[]> readCommand() throws IOException {
        int firstByte = inputStream.read();
        if (firstByte == -1) {
            //连接已关闭
            return null;
        }

        char type = (char) firstByte;
        if (type == '*') {
            return parseArray();
        } else {
            throw new IOException("Unsupported command format. Expected an Array ('*').");
        }
    }

    private List<byte[]> parseArray() throws IOException {
        //读取数组长度
        int arraySize = Integer.parseInt(readLine());
        if (arraySize <= 0) {
            return new ArrayList<>();
        }
        //循环读取数组中的每一个元素
        List<byte[]> commandParts = new ArrayList<>(arraySize);
        for (int i = 0; i < arraySize; i++) {
            int typeByte = inputStream.read();
            char elementType = (char) typeByte;
            if (elementType == '$') {
                commandParts.add(parseBulkString());
            } else {
                throw new IOException("Unsupported element type in Array. Expected Bulk String ('$').");
            }
        }
        return commandParts;
    }

    private byte[] parseBulkString() throws IOException {
        //读取字符串长度
        int stringLength = Integer.parseInt(readLine());
        if (stringLength == -1) {
            return null;
        }

        //读取指定长度的字节数据
        byte[] data = new byte[stringLength];
        int bytesRead = 0;
        while (bytesRead < stringLength) {
            int read = inputStream.read(data, bytesRead, stringLength - bytesRead);
            if (read == -1) {
                throw new IOException("Unexpected end of stream while reading Bulk String data.");
            }
            bytesRead += read;
        }

        if (inputStream.read() != '\r' || inputStream.read() != '\n') {
            throw new IOException("Expected CRLF after Bulk String data.");
        }

        return data;
    }
    /*
    * 从输入流中读取一行，以\r\n结尾
    * @return 读取到的行内容 (不包含 \r\n)
    * */
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
    public byte[] readRdbFile() throws IOException{
        if(inputStream.read()=='$'){
            return parseBulkString();
        }
        return null;
    }
}
