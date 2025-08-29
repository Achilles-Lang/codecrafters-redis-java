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
        // 对于一个普通的客户端连接，REPLCONF 命令只应该返回 OK
        // GETACK 的逻辑现在完全由 MasterConnectionHandler 自己处理
        return "OK";
    }
}
