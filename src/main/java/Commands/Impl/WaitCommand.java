package Commands.Impl;

import Commands.Command;
import Storage.DataStore;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author Achilles
 * 实现WAIT命令的初始版本
 */
public class WaitCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, OutputStream os) {
        if (args.size() != 2) {
            return new Exception("wrong number of arguments for 'wait' command");
        }

        try {
            // 解析需要的副本数量和超时时间
            int numReplicasToWaitFor = Integer.parseInt(new String(args.get(0), StandardCharsets.UTF_8));
            long timeout = Long.parseLong(new String(args.get(1), StandardCharsets.UTF_8));

            DataStore dataStore = DataStore.getInstance();

            int connectedReplicas = dataStore.getReplicas().size();



            return (long)connectedReplicas;

        } catch (NumberFormatException e) {
            return new Exception("value is not an integer or out of range");
        }
    }
}
