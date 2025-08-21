package Service;

import Config.WrongTypeException;
import Storage.StreamEntryID;
import Storage.ValueEntry;

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
            // **关键修复**: 添加缺失的 CRLF
            os.write("\r\n".getBytes());
        } else if (result instanceof Long || result instanceof Integer) {
            os.write((":" + result + "\r\n").getBytes()); // Integer
        } else if (result instanceof List) {
            List<?> list = (List<?>) result;
            // Write the array header, e.g., *2\r\n
            os.write(("*"+ list.size() + "\r\n").getBytes(StandardCharsets.UTF_8));

            // Recursively encode each item in the list
            for (Object item : list) {
                // This recursive call is powerful. It allows you to handle nested arrays in the future.
                encode(os, item);
            }
        } else if (result instanceof StreamEntryID) {
            String idStr = result.toString();
            encode(os, idStr.getBytes(StandardCharsets.UTF_8));
        } else if (result instanceof ValueEntry) {
            encode(os, ((ValueEntry) result).value);
        } else if (result instanceof Exception) {
            String message = ((Exception) result).getMessage();
            if (result instanceof WrongTypeException) { // 确保包名正确
                os.write(("-WRONGTYPE " + message + "\r\n").getBytes());
            } else {
                os.write(("-ERR " + message + "\r\n").getBytes());
            }
        }
    }
}
