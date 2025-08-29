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
            os.write("*-1\r\n".getBytes(StandardCharsets.UTF_8)); // RESP Null Bulk String
        } else if (result instanceof String) {
            os.write(("+" + result + "\r\n").getBytes(StandardCharsets.UTF_8)); // Simple String
        } else if (result instanceof byte[]) {
            byte[] arr = (byte[]) result;
            os.write(('$' + String.valueOf(arr.length) + "\r\n").getBytes(StandardCharsets.UTF_8));
            os.write(arr);
            os.write("\r\n".getBytes(StandardCharsets.UTF_8));
        } else if (result instanceof Long || result instanceof Integer) {
            os.write((":" + result + "\r\n").getBytes(StandardCharsets.UTF_8)); // Integer
        } else if (result instanceof List) {
            List<?> list = (List<?>) result;
            os.write(("*"+ list.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (Object item : list) {
                encode(os, item);
            }
        } else if (result instanceof StreamEntryID) {
            String idStr = result.toString();
            encode(os, idStr.getBytes(StandardCharsets.UTF_8));
        } else if (result instanceof ValueEntry) {
            encode(os, ((ValueEntry) result).value);
        } else if (result instanceof Exception) {
            String message = ((Exception) result).getMessage();
            if (result instanceof WrongTypeException) {
                os.write(("-WRONGTYPE " + message + "\r\n").getBytes(StandardCharsets.UTF_8));
            } else {
                os.write(("-ERR " + message + "\r\n").getBytes(StandardCharsets.UTF_8));
            }
        } else {
            // **关键修复**: 添加一个默认分支来处理所有未知类型，防止静默失败。
            String errorMsg = "Unsupported response type: " + result.getClass().getName();
            os.write(("-ERR " + errorMsg + "\r\n").getBytes(StandardCharsets.UTF_8));
        }
    }
}
