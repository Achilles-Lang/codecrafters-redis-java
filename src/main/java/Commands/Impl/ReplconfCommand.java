package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ReplconfCommand implements Command {

    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if (args.isEmpty()) {
            return new Exception("wrong number of arguments for 'replconf' command");
        }

        String subCommand = new String(args.get(0), StandardCharsets.UTF_8).toLowerCase();

        if ("getack".equals(subCommand)) {
            // ** ===> 关键修改 <=== **
            // 不再返回硬编码的 0，而是从上下文中获取真实的偏移量
            long offset = context.getReplicaOffset();

            String response = "*3\r\n" +
                    "$8\r\nREPLCONF\r\n" +
                    "$3\r\nACK\r\n" +
                    "$" + String.valueOf(offset).length() + "\r\n" +
                    offset + "\r\n";
            try {
                // 我们自己发送响应，所以返回 null 告诉主循环不要再发了
                context.getOutputStream().write(response.getBytes(StandardCharsets.UTF_8));
                context.getOutputStream().flush();
                return null;
            } catch (IOException e) {
                return e;
            }
        }

        // 处理其他 REPLCONF 子命令，比如 listening-port, capa
        return "OK";
    }
}
