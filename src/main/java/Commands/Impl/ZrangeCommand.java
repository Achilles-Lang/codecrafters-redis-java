package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Storage.DataStore;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZrangeCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if (args.size() != 3) {
            return new Exception("wrong number of arguments for 'zrange' command");
        }

        try {
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            int start = Integer.parseInt(new String(args.get(1), StandardCharsets.UTF_8));
            int stop = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));

            DataStore dataStore = DataStore.getInstance();
            // 直接返回 DataStore 的结果，它本身就是一个 List<byte[]>
            return dataStore.zrange(key, start, stop);

        } catch (NumberFormatException e) {
            return new Exception("value is not an integer or out of range");
        } catch (Exception e) {
            return e;
        }
    }

}
