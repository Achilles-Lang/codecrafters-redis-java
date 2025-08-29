// 文件路径: src/main/java/Service/RespEncoder.java
package Service;

import Commands.Command; // <--- 确保导入 Command 接口
import Config.WrongTypeException;
import Storage.StreamEntryID;
import Storage.ValueEntry;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RespEncoder {
    public static void encode(OutputStream os, Object result) throws IOException {

        // ===> 修改/新增的逻辑开始 <===
        if (result == Command.NULL_BULK_STRING_RESPONSE) {
            os.write("$-1\r\n".getBytes(StandardCharsets.UTF_8));
        } else if (result == Command.NULL_ARRAY_RESPONSE) {
            os.write("*-1\r\n".getBytes(StandardCharsets.UTF_8));
        }
        // ===> 修改/新增的逻辑结束 <===

        else if (result == null) {
            // 保留一个默认的 null 处理，以兼容老的 GET 命令逻辑
            os.write("$-1\r\n".getBytes(StandardCharsets.UTF_8));
        } else if (result instanceof String) {
            os.write(("+" + result + "\r\n").getBytes(StandardCharsets.UTF_8));
        } else if (result instanceof byte[]) {
            byte[] arr = (byte[]) result;
            os.write(('$' + String.valueOf(arr.length) + "\r\n").getBytes(StandardCharsets.UTF_8));
            os.write(arr);
            os.write("\r\n".getBytes(StandardCharsets.UTF_8));
        } else if (result instanceof Long || result instanceof Integer) {
            os.write((":" + result + "\r\n").getBytes(StandardCharsets.UTF_8));
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
            String errorMsg = "Unsupported response type: " + result.getClass().getName();
            os.write(("-ERR " + errorMsg + "\r\n").getBytes(StandardCharsets.UTF_8));
        }
    }
}
