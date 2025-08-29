package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Storage.DataStore;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author Achilles
 * 实现Zrem 命令
 */
public class ZremCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if(args.size() < 2){
            return new Exception("wrong number of arguments for 'zrem' command");
        }
        try {
            String key=new String(args.get(0), StandardCharsets.UTF_8);
            List<byte[]> membersToRemove = args.subList(1, args.size());

            DataStore dataStore = DataStore.getInstance();
            long removedCount = dataStore.zrem(key, membersToRemove);

            return removedCount;
        } catch (Exception e){
            return e;
        }
    }
}
