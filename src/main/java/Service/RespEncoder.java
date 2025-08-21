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

            if(!list.isEmpty()&&list.get(0) instanceof byte[]){
                os.write(("*" + list.size() + "\r\n").getBytes());
                for (Object item : list) {
                    byte[] part=(byte[]) item;
                    os.write(('$' + String.valueOf(part.length) + "\r\n").getBytes());
                    os.write(part);
                    os.write("\r\n".getBytes());
                }
                return;
            }
        } else if (result instanceof StreamEntryID) {
            String idStr = result.toString();
            encode(os, idStr.getBytes(StandardCharsets.UTF_8));
        } else if (result instanceof Exception) {
            os.write(("-ERR " + ((Exception) result).getMessage() + "\r\n").getBytes());
        }
    }
}
