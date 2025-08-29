package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Storage.DataStore;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZcardCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if(args.size()!=1){
            return new Exception("wrong number of arguments for 'zcard' command");
        }
        try {
            String key=new String(args.get(0), StandardCharsets.UTF_8);
            DataStore dataStore=DataStore.getInstance();
            return dataStore.zcard(key);
        } catch (Exception e){
            return e;
        }
    }
}
