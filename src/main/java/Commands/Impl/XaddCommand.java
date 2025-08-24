package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Commands.WriteCommand;
import Storage.DataStore;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Achilles
 */
public class XaddCommand implements WriteCommand {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        System.out.println("[DEBUG] XaddCommand: Starting execution.");
        try {
            if (args.size() < 4 || args.size() % 2 != 0) {
                System.out.println("[DEBUG] XaddCommand: Argument check failed.");
                return new Exception("wrong number of arguments for 'xadd' command");
            }

            String key = new String(args.get(0), StandardCharsets.UTF_8);
            String idStr = new String(args.get(1), StandardCharsets.UTF_8);
            System.out.println("[DEBUG] XaddCommand: Key='" + key + "', ID='" + idStr + "'");

            long timestamp;
            int sequence;

            if ("*".equals(idStr)) {
                timestamp = -1;
                sequence = -1;
            } else if (idStr.endsWith("-*")) {
                timestamp = Long.parseLong(idStr.substring(0, idStr.length() - 2));
                sequence = -1;
            } else {
                String[] idParts = idStr.split("-");
                timestamp = Long.parseLong(idParts[0]);
                sequence = Integer.parseInt(idParts[1]);
            }
            System.out.println("[DEBUG] XaddCommand: Parsed ID to timestamp=" + timestamp + ", sequence=" + sequence);

            Map<String, byte[]> fields = new HashMap<>();
            for (int i = 2; i < args.size(); i += 2) {
                String fieldKey = new String(args.get(i), StandardCharsets.UTF_8);
                byte[] fieldValue = args.get(i + 1);
                fields.put(fieldKey, fieldValue);
            }
            System.out.println("[DEBUG] XaddCommand: Parsed " + fields.size() + " fields. Calling DataStore...");

            Object result = DataStore.getInstance().xadd(key, timestamp, sequence, fields);

            System.out.println("[DEBUG] XaddCommand: DataStore.xadd executed successfully. Returning result.");
            return result;

        } catch (RuntimeException e) {
            // **这是最重要的部分**：捕获所有运行时异常并打印堆栈跟踪
            System.out.println("[DEBUG] XaddCommand: CRASH! A RuntimeException was caught!");
            e.printStackTrace(System.out); // 这会打印出详细的错误信息和行号
            return new Exception("A runtime error occurred in XaddCommand: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("[DEBUG] XaddCommand: A checked Exception was caught: " + e.getMessage());
            return e;
        }
    }
}
