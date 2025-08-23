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

    public RdbParser(File rdbFile,DataStore dataStore){
        this.rdbFile=rdbFile;
        this.dataStore=dataStore;
    }

    public void parse() throws IOException{
        try (FileInputStream fis=new FileInputStream(rdbFile)) {
            this.bis=new BufferedInputStream(fis);

            verifyHeader();

            long expiryTime=-1;

            while (true) {
                int opCode=bis.read();
                if(opCode==-1){
                    break;
                }

                switch (opCode) {
                    case 0xFE: // 选择数据库
                        readDbSelector();
                        break;
                    case 0xFB: // 调整哈希表大小
                        readResizeDb();
                        break;
                    case 0xFD: // 过期时间 (秒)
                        expiryTime = readExpirySeconds();
                        break;
                    case 0xFC: // 过期时间 (毫秒)
                        expiryTime = readExpiryMillis();
                        break;
                    case 0x00: // 字符串类型的值
                        readKeyValuePair(expiryTime);
                        expiryTime = -1; // 重置过期时间
                        break;
                    case 0xFF: // 文件结束符
                        return; // 成功解析
                    default:
                        // 对于此阶段，我们可以忽略其他操作码
                        // System.out.println("Unknown opcode: " + opCode);
                        break;
                }
            }
        }
    }

    private void verifyHeader() throws IOException{
        byte[] header=bis.readNBytes(9);
        String magic=new String(header,0,5, StandardCharsets.UTF_8);
        if (!"REDIS".equals(magic)) {
            throw new IOException("Invalid RDB file format: Magic string is incorrect.");
        }
    }

    private void readDbSelector() throws IOException{
        readLengthEncodedInt();
    }

    private void readResizeDb() throws IOException{
        readLengthEncodedInt();
        readLengthEncodedInt();
    }

    private long readExpirySeconds() throws IOException{
        byte[] bytes=bis.readNBytes(4);
        long seconds = ((long) (bytes[3] & 0xFF) << 24) |
                ((long) (bytes[2] & 0xFF) << 16) |
                ((long) (bytes[1] & 0xFF) << 8)  |
                ((long) (bytes[0] & 0xFF));
        return seconds * 1000;
    }

    private long readExpiryMillis() throws IOException{
        byte[] bytes=bis.readNBytes(8);
        return ((long) (bytes[7] & 0xFF) << 56) |
                ((long) (bytes[6] & 0xFF) << 48) |
                ((long) (bytes[5] & 0xFF) << 40) |
                ((long) (bytes[4] & 0xFF) << 32) |
                ((long) (bytes[3] & 0xFF) << 24) |
                ((long) (bytes[2] & 0xFF) << 16) |
                ((long) (bytes[1] & 0xFF) << 8)  |
                ((long) (bytes[0] & 0xFF));
    }

    private  void readKeyValuePair(long expireTime) throws IOException{
        String key=readString();
        String value=readString();

        dataStore.setString(key, new ValueEntry(value.getBytes(StandardCharsets.UTF_8), expireTime));
    }

    private String readString() throws IOException{
        int length=readLengthEncodedInt();
        byte[] bytes= bis.readNBytes(length);
        return new String(bytes,StandardCharsets.UTF_8);
    }

    private int readLengthEncodedInt() throws IOException{
        int firstByte=bis.read();

        return firstByte & 0x3F;
    }
}
