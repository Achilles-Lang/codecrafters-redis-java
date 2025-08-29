// 文件路径: src/main/java/Commands/Impl/BlpopCommand.java

package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Storage.DataStore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BlpopCommand implements Command {

    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if (args.size() < 2) {
            return new Exception("wrong number of arguments for 'blpop' command");
        }

        try {
            // 解析超时时间和 keys
            double timeout = Double.parseDouble(new String(args.get(args.size() - 1), StandardCharsets.UTF_8));
            List<byte[]> keys = args.subList(0, args.size() - 1);

            // 调用 DataStore 的阻塞方法
            Object[] result = DataStore.getInstance().blpop(keys, timeout);

            // ===> 核心修正：不再自己发送响应，而是返回结果或信号 <===
            if (result == null) {
                // 如果结果是 null (超时)，返回我们定义的 Null Array 信号
                return Command.NULL_ARRAY_RESPONSE;
            } else {
                // 如果成功获取到数据，将其构造成一个列表并返回
                // 主循环和 RespEncoder 会自动将其编码为 RESP 数组
                List<byte[]> responseList = new ArrayList<>();
                if (result.length == 2 && result[0] instanceof byte[] && result[1] instanceof byte[]) {
                    responseList.add((byte[]) result[0]); // key
                    responseList.add((byte[]) result[1]); // value
                    return responseList;
                } else {
                    // 理论上不应该发生，但作为保护
                    return new Exception("Internal error: DataStore returned unexpected format for BLPOP");
                }
            }
        } catch (InterruptedException e) {
            // 如果线程在等待时被中断，也返回 Null Array
            Thread.currentThread().interrupt(); // 重新设置中断状态
            return Command.NULL_ARRAY_RESPONSE;
        } catch (Exception e) {
            // 捕获其他所有异常，比如数字格式错误
            return e;
        }
    }
}
