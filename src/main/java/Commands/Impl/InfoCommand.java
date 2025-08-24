package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Storage.DataStore;
import Storage.ReplicationInfo;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.StringJoiner;

/**
 * @author Achilles
 */
public class InfoCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
// INFO 命令可以不带参数，也可以带一个参数 "replication"
        if (args.size() > 1) {
            return new Exception("wrong number of arguments for 'info' command");
        }

        // 默认只处理 replication 部分
        // 即使没有参数，或者参数不是 "replication"，对于此阶段我们都返回角色信息
        if (args.isEmpty() || "replication".equalsIgnoreCase(new String(args.get(0), StandardCharsets.UTF_8))) {

            ReplicationInfo info=DataStore.getInstance().getReplicationInfo();

            StringJoiner sj=new StringJoiner("\r\n");
            sj.add("role:"+info.getRole());
            sj.add("master_replid:" + info.getMasterReplid());
            sj.add("master_repl_offset:" + info.getMasterReplOffset());

            return  sj.toString().getBytes(StandardCharsets.UTF_8);
        }

        // 如果是其他 section，可以返回空字符串
        return "".getBytes(StandardCharsets.UTF_8);    }
}
