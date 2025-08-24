package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Achilles
 * 实现了 PING 命令，完全无状态。
 */
public class PingCommand implements Command {

    // **关键修改**: 移除了所有成员变量 (isClientSubscribed) 和 setter 方法。

    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        // **关键修改**: 从传入的 context 对象中获取客户端状态
        if (context.isClientSubscribed()) {
            // 订阅模式下的响应
            List<byte[]> response = new ArrayList<>();
            response.add("pong".getBytes(StandardCharsets.UTF_8));

            if (args.isEmpty()) {
                response.add("".getBytes(StandardCharsets.UTF_8));
            } else {
                response.add(args.get(0));
            }
            return response;

        } else {
            // 普通模式下的响应
            if (args.isEmpty()) {
                return "PONG";
            } else {
                return args.get(0);
            }
        }
    }
}
