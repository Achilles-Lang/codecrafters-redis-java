package Commands.Impl;

import Commands.Command;
import Config.WrongTypeException;
import Storage.DataStore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BlpopCommand implements Command {
    @Override
    public Object execute(List<byte[]> args) {
        if (args.size() < 2) {
            return new Exception("wrong number of arguments for 'blpop' command");
        }
        try {
            double timeout = Double.parseDouble(new String(args.get(args.size() - 1)));
            List<byte[]> keys = args.subList(0, args.size() - 1);
            Object[] result = DataStore.getInstance().blpop(keys, timeout);
            if (result == null) {
                return null;
            } else {
                return Arrays.asList(result);
            }
        } catch (Exception e) {
            return e;
        }
    }
}
