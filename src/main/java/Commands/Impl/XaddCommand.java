package Commands.Impl;

import Commands.Command;
import Storage.DataStore;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XaddCommand implements Command {
    @Override
    public Object execute(List<byte[]> args) {
        if (args.size() < 4 || args.size() % 2 != 0) {
            return new Exception("wrong number of arguments for 'xadd' command");
        }
        try {
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            String idStr = new String(args.get(1), StandardCharsets.UTF_8);

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

            Map<String, byte[]> fields = new HashMap<>();
            for (int i = 2; i < args.size(); i += 2) {
                String fieldKey = new String(args.get(i), StandardCharsets.UTF_8);
                byte[] fieldValue = args.get(i + 1);
                fields.put(fieldKey, fieldValue);
            }

            return DataStore.getInstance().xadd(key, timestamp, sequence, fields);

        } catch (NumberFormatException e) {
            return new Exception("Invalid stream ID specified as XADD argument");
        } catch (Exception e) {
            return e;
        }
    }
}
