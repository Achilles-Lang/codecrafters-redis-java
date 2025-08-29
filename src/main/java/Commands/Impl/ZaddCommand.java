package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Storage.DataStore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Achilles
 * 实现ZADD 命令
 */
public class ZaddCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if (args.size()<3||args.size()%2!=1){
            return new Exception("wrong number of arguments for 'zadd' command");
        }

        String key = new String(args.get(0), StandardCharsets.UTF_8);
        List<Object> scoresAndMembers = new ArrayList<>();

        for(int i=1;i<args.size();i+=2){
            scoresAndMembers.add(args.get(i));
            scoresAndMembers.add(args.get(i+1));
        }

        try {
            DataStore dataStore = DataStore.getInstance();
            int addedCount = dataStore.zadd(key, scoresAndMembers);
            return addedCount;
        } catch (Exception e) {
            return e;
        }
    }
}
