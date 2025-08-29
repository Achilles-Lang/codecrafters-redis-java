// File Path: src/main/java/Commands/Impl/ReplconfCommand.java

package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ReplconfCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if (args.isEmpty()) {
            return new Exception("wrong number of arguments for 'replconf' command");
        }

        String subCommand = new String(args.get(0), StandardCharsets.UTF_8).toLowerCase();

        // This command is special. The replica receives it from the master
        // and must reply with its current processed offset.
        if ("getack".equals(subCommand)) {
            // The offset is passed via the context from the MasterConnectionHandler
            long offset = context.getReplicaOffset();
            String response = "*3\r\n" +
                    "$8\r\nREPLCONF\r\n" +
                    "$3\r\nACK\r\n" +
                    "$" + String.valueOf(offset).length() + "\r\n" +
                    offset + "\r\n";
            try {
                // The context must have a valid output stream to the master
                if (context.getOutputStream() != null) {
                    context.getOutputStream().write(response.getBytes(StandardCharsets.UTF_8));
                    context.getOutputStream().flush();
                }
            } catch (IOException e) {
                System.out.println("Error sending ACK reply: " + e.getMessage());
            }
            // We've handled the response ourselves, so we return a special value
            // to prevent the main loop from sending another response.
            return Command.STATE_CHANGE_SUBSCRIBE; // Or any other "no-reply" signal object
        }

        // This handles the initial handshake REPLCONF calls from the replica to the master.
        // It's not used during the propagation phase.
        return "OK";
    }
}
