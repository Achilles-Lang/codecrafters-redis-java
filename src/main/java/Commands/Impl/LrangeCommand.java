package Commands.Impl;

import Commands.Command;
import Config.WrongTypeException;
import Service.ClientHandler;
import Storage.DataStore;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class LrangeCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, ClientHandler clientHandler) {
        if (args.size() != 3) {
            return new Exception("wrong number of arguments for 'lrange' command");
        }
        try {
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            int start = Integer.parseInt(new String(args.get(1)));
            int end = Integer.parseInt(new String(args.get(2)));
            return DataStore.getInstance().lrange(key, start, end);
        } catch (NumberFormatException e) {
            return new Exception("value is not an integer or out of range");
        } catch (WrongTypeException e) {
            return e;
        }
    }
}
