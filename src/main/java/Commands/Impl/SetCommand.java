package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Commands.WriteCommand;
import Storage.DataStore;
import Storage.ValueEntry;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author Achilles
 */
public class SetCommand implements WriteCommand {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if (args.size() < 2) {
            return new Exception("wrong number of arguments for 'set' command");
        }
        String key = new String(args.get(0), StandardCharsets.UTF_8);
        byte[] value = args.get(1);
        long ttl = -1;

        if (args.size() > 3) {
            String option = new String(args.get(2), StandardCharsets.UTF_8);
            if ("px".equalsIgnoreCase(option)) {
                try {
                    ttl = Long.parseLong(new String(args.get(3)));
                } catch (NumberFormatException e) {
                    return new Exception("value is not an integer or out of range");
                }
            }
        }

        long expiryTimestamp = (ttl > 0) ? (System.currentTimeMillis() + ttl) : -1;
        DataStore.getInstance().setString(key, new ValueEntry(value, expiryTimestamp));

        return "OK";
    }
}
