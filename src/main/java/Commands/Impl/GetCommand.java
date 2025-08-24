package Commands.Impl;

import Commands.CommandContext;
import Storage.ValueEntry;
import Commands.Command;
import Storage.DataStore;

import java.io.OutputStream;
import java.util.List;

/**
 * @author Achilles
 */
public class GetCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if(args.size() != 1){
            return new Exception("ERR wrong number of arguments for 'get' command");
        }
        String key = new String(args.get(0));
        ValueEntry entry = DataStore.getInstance().getString(key);
        return (entry!=null)?entry.value:null;
    }
}
