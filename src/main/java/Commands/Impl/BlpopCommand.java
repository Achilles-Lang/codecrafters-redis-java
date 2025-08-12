package Commands.Impl;

import Commands.Command;
import Config.WrongTypeException;
import Storage.DataStore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BlpopCommand implements Command {
    @Override
    public Object execute(List<byte[]> args) {
        if (args.size() != 2) {
            return new Exception("wrong number of arguments for 'blpop' command");
        }
        try {
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            double timeout = Double.parseDouble(new String(args.get(1)));

            // 调用 DataStore 的阻塞方法
            byte[] poppedValue = DataStore.getInstance().blpop(key, timeout);

            if (poppedValue == null) {
                // 如果 DataStore 返回 null (超时或key不存在)，我们也返回 null。
                // RespEncoder 会将 null 编码为 "$-1\r\n" (NIL)。
                return null;
            } else {
                // 如果成功弹出一个值，需要返回一个包含 key 和 value 的二元数组
                List<Object> responseArray = new ArrayList<>();
                responseArray.add(args.get(0)); // 1. key
                responseArray.add(poppedValue);      // 2. value
                return responseArray;
            }

        } catch (NumberFormatException e) {
            return new Exception("timeout is not a valid float");
        } catch (Exception e) {
            return e;
        }    }
}
