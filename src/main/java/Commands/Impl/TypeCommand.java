package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Storage.DataStore;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TypeCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if (args.size() != 1) {
            return new Exception("wrong number of arguments for 'type' command");
        }
        String key = new String(args.get(0), StandardCharsets.UTF_8);
        return DataStore.getInstance().getType(key);
    }
}
