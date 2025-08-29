package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Storage.DataStore;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author Achilles
 * 实现Zscore命令
 */
public class ZscoreCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if(args.size()!=2){
            return new Exception("wrong number of arguments for 'zscore' command");
        }
        try {
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            byte[] member = args.get(1);

            DataStore dataStore = DataStore.getInstance();
            Double score = dataStore.zscore(key, member);

            if(score==null){
                return Command.NULL_BULK_STRING_RESPONSE;
            }
            return String.valueOf(score).getBytes(StandardCharsets.UTF_8);
        } catch (Exception e){
            return e;
        }
    }
}
