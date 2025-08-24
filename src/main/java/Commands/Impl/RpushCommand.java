package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Commands.WriteCommand;
import Config.WrongTypeException;
import Storage.DataStore;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RpushCommand implements WriteCommand {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if (args.size() < 2) {
            return new Exception("wrong number of arguments for 'rpush' command");
        }
        String key = new String(args.get(0), StandardCharsets.UTF_8);
        List<byte[]> values = args.subList(1, args.size());
        try {
            return DataStore.getInstance().rpush(key, values);
        } catch (WrongTypeException e) {
            return e;
        }
    }
}
