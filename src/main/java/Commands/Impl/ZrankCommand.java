package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Storage.DataStore;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZrankCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if(args.size()!=2){
            return new Exception("wrong number of arguments for 'zrank' command");
        }
        try {
            String key=new String(args.get(0), StandardCharsets.UTF_8);
            byte[] member=args.get(1);

            DataStore dataStore=DataStore.getInstance();
            Long rank=dataStore.zrank(key,member);

            if(rank==null){
                return Command.NULL_BULK_STRING_RESPONSE;
            }
            return rank;
        } catch (Exception e){
            return e;
        }
    }
}
