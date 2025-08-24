package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Commands.WriteCommand;
import Config.WrongTypeException;
import Storage.DataStore;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class LpopCommand implements WriteCommand {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if (args.size() < 1 || args.size() > 2) {
            return new Exception("wrong number of arguments for 'lpop' command");
        }
        try {
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            int count = 1;
            boolean countProvided = args.size() == 2;

            if (countProvided) {
                count = Integer.parseInt(new String(args.get(1)));
                if (count < 0) {
                    return new Exception("value is out of range, must be positive");
                }
            }

            List<byte[]> poppedValues = DataStore.getInstance().lpop(key, count);

            if (poppedValues == null) {
                return null; // Key didn't exist
            }

            if (!countProvided) {
                // 如果用户没提供 count，则返回单个值或 NIL
                return poppedValues.isEmpty() ? null : poppedValues.get(0);
            } else {
                // 如果用户提供了 count，则总是返回数组
                return poppedValues;
            }
        } catch (NumberFormatException e) {
            return new Exception("value is not an integer or out of range");
        } catch (WrongTypeException e) {
            return e;
        }
    }
}
