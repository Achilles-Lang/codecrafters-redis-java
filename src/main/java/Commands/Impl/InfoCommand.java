package Commands.Impl;

import Commands.Command;
import Storage.DataStore;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author Achilles
 */
public class InfoCommand implements Command {
    @Override
    public Object execute(List<byte[]> args) {
// INFO 命令可以不带参数，也可以带一个参数 "replication"
        if (args.size() > 1) {
            return new Exception("wrong number of arguments for 'info' command");
        }

        // 默认只处理 replication 部分
        // 即使没有参数，或者参数不是 "replication"，对于此阶段我们都返回角色信息
        if (args.isEmpty() || "replication".equalsIgnoreCase(new String(args.get(0), StandardCharsets.UTF_8))) {

            // 1. 从 DataStore 获取角色信息
            String role = DataStore.getInstance().getRole();

            // 2. 构建响应字符串
            String replicationInfo = "role:" + role;

            // 3. 将字符串作为 byte[] 返回，RespEncoder 会自动将其编码为 Bulk String
            return replicationInfo.getBytes(StandardCharsets.UTF_8);
        }

        // 如果是其他 section，可以返回空字符串
        return "".getBytes(StandardCharsets.UTF_8);    }
}
