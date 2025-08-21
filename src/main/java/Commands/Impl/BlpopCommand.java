package Commands.Impl;

import Commands.Command;
import Config.WrongTypeException;
import Storage.DataStore;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Achilles
 */
public class BlpopCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, OutputStream os) {
        if (args.size() < 2) {
            return new Exception("wrong number of arguments for 'blpop' command");
        }
        try {
            // BLPOP key1 [key2 ...] timeout
            double timeout = Double.parseDouble(new String(args.get(args.size() - 1)));
            List<byte[]> keys = args.subList(0, args.size() - 1);

            Object[] result = DataStore.getInstance().blpop(keys, timeout);

            if (result == null) {
                // DataStore 返回 null 代表超时
                return null;
            } else {
                // 成功弹出，返回二元数组
                return Arrays.asList(result);
            }

        } catch (InterruptedException e) {
            // **关键修复**：当线程被中断时（通常意味着超时），返回 null (会被编码为 NIL)
            // 恢复线程的中断状态，这是一个好习惯
            Thread.currentThread().interrupt();
            return null;
        } catch (WrongTypeException e) {
            // 对于类型错误，返回错误是正确的
            return e;
        } catch (NumberFormatException e) {
            // 对于参数格式错误，返回错误也是正确的
            return new Exception("timeout is not a valid float");
        }
    }
}
