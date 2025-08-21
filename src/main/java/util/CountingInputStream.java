package util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public class CountingInputStream extends FileInputStream {
    private long count = 0;

    public CountingInputStream(InputStream in) throws FileNotFoundException {
        super(in.toString());
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

    /**
     * 获取当前已读取的总字节数。
     * @return 已读取的字节总数。
     */
    public long getCount() {
        return count;
    }

    /**
     * 重置字节计数器。
     */
    public void resetCount() {
        this.count = 0;
    }
}
