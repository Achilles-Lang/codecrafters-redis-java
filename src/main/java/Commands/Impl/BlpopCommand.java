package Commands.Impl;

import Commands.Command;
import Storage.DataStore;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
            // **KEY FIX**: Changed back to Double.parseDouble to handle decimal timeouts like "0.3"
            double timeout = Double.parseDouble(new String(args.get(args.size() - 1), StandardCharsets.UTF_8));
            List<byte[]> keys = args.subList(0, args.size() - 1);
            String keysStr = new String(keys.get(0), StandardCharsets.UTF_8);

            System.out.println("[BlpopCommand][Thread-" + threadId + "] Calling DataStore.blpop for key: " + keysStr);
            Object[] result = DataStore.getInstance().blpop(keys, timeout);
            System.out.println("[BlpopCommand][Thread-" + threadId + "] DataStore.blpop returned.");

            if (result == null) {
                System.out.println("[BlpopCommand][Thread-" + threadId + "] Result is null (timeout). Returning null.");
                return null; // This will be encoded as a RESP Null
            } else {
                System.out.println("[BlpopCommand][Thread-" + threadId + "] Result is not null. Formatting response.");
                if (result.length == 2 && result[0] instanceof byte[] && result[1] instanceof byte[]) {
                    List<byte[]> responseList = new ArrayList<>();
                    responseList.add((byte[]) result[0]); // Add key
                    responseList.add((byte[]) result[1]); // Add value
                    return responseList;
                } else {
                    return new Exception("Internal error: DataStore returned unexpected format for BLPOP");
                }
            }
        } catch (NumberFormatException e) {
            return new Exception("ERR value is not a valid float or out of range");
        } catch (Exception e) {
            System.err.println("[BlpopCommand][Thread-" + threadId + "] An error occurred:");
            e.printStackTrace(System.err);
            return new Exception("Fatal error in BLPOP: " + e.getMessage());
        }
    }
}
