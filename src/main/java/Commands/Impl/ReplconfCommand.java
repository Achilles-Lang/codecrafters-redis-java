package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * @author Achilles
 * @create 2018/11/01
 * @description 配置复制
 */
public class ReplconfCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if (args.size() >= 2 && "getack".equalsIgnoreCase(new String(args.get(0)))) {
            // 从 DataStore 或者 MasterConnectionHandler 获取偏移量
            // 这里需要一种机制来传递偏移量，暂时我们先回复一个硬编码的 0
            long offset = 0; // TODO: Replace with actual offset
            String response = "*3\r\n" +
                    "$8\r\nREPLCONF\r\n" +
                    "$3\r\nACK\r\n" +
                    "$" + String.valueOf(offset).length() + "\r\n" +
                    offset + "\r\n";
            try {
                context.getOutputStream().write(response.getBytes());
                context.getOutputStream().flush();
            } catch (IOException e) {
                return e;
            }
            return null; // 我们已经自己回复了，不需要主循环再回复
        }

        return "OK"; // 对于 listening-port 等命
    }
}
