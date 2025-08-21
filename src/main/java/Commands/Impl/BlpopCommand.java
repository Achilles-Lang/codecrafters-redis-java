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
            // It's safer to parse timeout as long to avoid floating point issues
            long timeout = Long.parseLong(new String(args.get(args.size() - 1)));
            List<byte[]> keys = args.subList(0, args.size() - 1);
            String keysStr = new String(keys.get(0), StandardCharsets.UTF_8);

            System.out.println("[BlpopCommand][Thread-" + threadId + "] Calling DataStore.blpop for key: " + keysStr);
            // Assuming DataStore.blpop returns Object[] where elements are byte[]
            Object[] result = DataStore.getInstance().blpop(keys, timeout);
            System.out.println("[BlpopCommand][Thread-" + threadId + "] DataStore.blpop returned.");

            if (result == null) {
                System.out.println("[BlpopCommand][Thread-" + threadId + "] Result is null (timeout). Returning null.");
                return null; // This will be encoded as a RESP Null Bulk String
            } else {
                System.out.println("[BlpopCommand][Thread-" + threadId + "] Result is not null. Formatting response.");
                // **KEY CHANGE**: Convert the Object[] to a List<byte[]>
                // This is the precise format needed for a RESP Array response.
                List<byte[]> responseList = new ArrayList<>();
                for (Object item : result) {
                    if (item instanceof byte[]) {
                        responseList.add((byte[]) item);
                    } else if (item instanceof String) {
                        // Add flexibility in case your DataStore returns Strings
                        responseList.add(((String) item).getBytes(StandardCharsets.UTF_8));
                    }
                }
                return responseList;
            }
        } catch (NumberFormatException e) {
            return new Exception("ERR timeout is not an integer or out of range");
        } catch (Exception e) {
            System.err.println("[BlpopCommand][Thread-" + threadId + "] An error occurred:");
            e.printStackTrace(System.err);
            return new Exception("Fatal error in BLPOP: " + e.getMessage());
        }
    }
}
