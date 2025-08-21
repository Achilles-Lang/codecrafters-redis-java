package Commands.Impl;

import Commands.Command;
import Commands.WriteCommand;
import Config.WrongTypeException;
import Storage.DataStore;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class LpushCommand implements WriteCommand {
    @Override
    public Object execute(List<byte[]> args, OutputStream os) {
        if (args.size() < 2) {
            return new Exception("wrong number of arguments for 'lpush' command");
        }
        try {
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            List<byte[]> values = args.subList(1, args.size());
            return DataStore.getInstance().lpush(key, values);
        } catch (WrongTypeException e) {
            return e;
        }
    }
}
