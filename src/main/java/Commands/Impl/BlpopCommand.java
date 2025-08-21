package Commands.Impl;

import Commands.Command;
import Config.WrongTypeException;
import Storage.DataStore;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Achilles
 */
public class BlpopCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, OutputStream os) {
        long threadId = Thread.currentThread().getId();
        System.out.println("[BlpopCommand][Thread-" + threadId + "] ==> START");

        if (args.size() < 2) {
            return new Exception("wrong number of arguments for 'blpop' command");
        }

        try {
            double timeout = Double.parseDouble(new String(args.get(args.size() - 1), StandardCharsets.UTF_8));
            List<byte[]> keys = args.subList(0, args.size() - 1);
            String keysStr = new String(keys.get(0), StandardCharsets.UTF_8); // 简化日志

            System.out.println("[BlpopCommand][Thread-" + threadId + "] Calling DataStore.blpop for key: " + keysStr);
            Object[] result = DataStore.getInstance().blpop(keys, timeout);
            System.out.println("[BlpopCommand][Thread-" + threadId + "] DataStore.blpop returned.");

            if (result == null) {
                System.out.println("[BlpopCommand][Thread-" + threadId + "] Result is null (timeout). Returning null.");
                return null;
            } else {
                System.out.println("[BlpopCommand][Thread-" + threadId + "] Result is not null. Formatting response.");
                return Arrays.asList(result);
            }
        } catch (Throwable t) { // **关键**: 捕获所有类型的异常，包括 Error
            System.out.println("[BlpopCommand][Thread-" + threadId + "] !!! FATAL CRASH CAUGHT !!!");
            t.printStackTrace(System.out); // **关键**: 这会打印出完整的错误堆栈和行号
            return new Exception("Fatal error in BLPOP: " + t.getMessage());
        }
    }
}
