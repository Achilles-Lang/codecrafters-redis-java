// 文件路径: src/main/java/Commands/Impl/ReplconfCommand.java

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
            if (context.getParser() != null) {
                // ** ===> 核心修正：直接从 parser 获取当前的偏移量 <=== **
                long offset = context.getParser().getBytesRead();

                String response = "*3\r\n" +
                        "$8\r\nREPLCONF\r\n" +
                        "$3\r\nACK\r\n" +
                        "$" + String.valueOf(offset).length() + "\r\n" +
                        offset + "\r\n";
                try {
                    context.getOutputStream().write(response.getBytes(StandardCharsets.UTF_8));
                    context.getOutputStream().flush();
                } catch (IOException e) {
                    System.out.println("Error sending ACK reply: " + e.getMessage());
                }
            }
            // 已经手动回复，返回一个特殊对象，告诉主循环不要再回复
            return Command.NULL_ARRAY_RESPONSE; // 或者其他你定义的“无回复”信号
        }

        // 处理其他 REPLCONF 子命令，如 listening-port, capa
        return "OK";
    }
}
