package Service;

import Storage.StreamEntryID;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author Achilles
 * RESP 响应编码器
 */
public class RespEncoder {
    public static void encode(OutputStream os, Object result) throws IOException {
        if (result == null) {
            os.write("$-1\r\n".getBytes()); // NIL Bulk String
        } else if (result instanceof String) {
            os.write(("+" + result + "\r\n").getBytes()); // Simple String
        } else if (result instanceof byte[]) {
            byte[] arr = (byte[]) result;
            os.write(('$' + String.valueOf(arr.length) + "\r\n").getBytes());
            os.write(arr);
            os.write("\r\n".getBytes());
        } else if (result instanceof Long || result instanceof Integer) {
            os.write((":" + result + "\r\n").getBytes()); // Integer
        } else if (result instanceof List) {
            List<?> list = (List<?>) result;
            os.write(("*" + list.size() + "\r\n").getBytes());
            for (Object item : list) {
                // 递归编码数组中的每个元素
                encode(os, item);
            }
        } else if (result instanceof StreamEntryID) {
            String idStr = result.toString();
            encode(os, idStr.getBytes(StandardCharsets.UTF_8));
        } else if (result instanceof Exception) {
            os.write(("-ERR " + ((Exception) result).getMessage() + "\r\n").getBytes());
        }
    }
}
