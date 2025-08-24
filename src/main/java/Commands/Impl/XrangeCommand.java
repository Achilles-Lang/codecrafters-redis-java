package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Config.WrongTypeException;
import Storage.DataStore;
import Storage.StreamEntry;
import Storage.StreamEntryID;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class XrangeCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if (args.size() != 3) {
            return new Exception("wrong number of arguments for 'xrange' command");
        }
        try {
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            String startIdStr = new String(args.get(1), StandardCharsets.UTF_8);
            String endIdStr = new String(args.get(2), StandardCharsets.UTF_8);

            // 解析 ID，处理不完整的 ID
            StreamEntryID startId = parseId(startIdStr, true);
            StreamEntryID endId = parseId(endIdStr, false);

            // 调用 DataStore 获取原始数据
            List<StreamEntry> entries = DataStore.getInstance().xrange(key, startId, endId);

            // 将 DataStore 返回的结果转换为 RESP 嵌套数组格式
            List<List<Object>> response = new ArrayList<>();
            for (StreamEntry entry : entries) {
                // 内层数组 1: 代表一个条目
                List<Object> entryResponse = new ArrayList<>();
                entryResponse.add(entry.id.toString().getBytes()); // ID

                // 内层数组 2: 代表条目中的键值对
                List<byte[]> fieldsResponse = new ArrayList<>();
                for (Map.Entry<String, byte[]> field : entry.fields.entrySet()) {
                    fieldsResponse.add(field.getKey().getBytes());
                    fieldsResponse.add(field.getValue());
                }
                entryResponse.add(fieldsResponse);

                response.add(entryResponse);
            }
            return response;

        } catch (WrongTypeException e) {
            return e;
        } catch (Exception e) {
            return new Exception("Invalid stream ID specified as XRANGE argument");
        }
    }
    /**
     * 辅助方法，用于解析可能不完整的 Stream ID。
     */
    public static StreamEntryID parseId(String idStr, boolean isStart) {
        if ("-".equals(idStr)) {
            return new StreamEntryID(0, 0); // 简化处理，代表最小
        }
        if ("+".equals(idStr)) {
            return new StreamEntryID(Long.MAX_VALUE, Integer.MAX_VALUE); // 简化处理，代表最大
        }

        String[] parts = idStr.split("-");
        long timestamp = Long.parseLong(parts[0]);
        int sequence;

        if (parts.length > 1) {
            sequence = Integer.parseInt(parts[1]);
        } else {
            // 如果不带序列号，根据是 start 还是 end 进行补全
            sequence = isStart ? 0 : Integer.MAX_VALUE;
        }
        return new StreamEntryID(timestamp, sequence);
    }
}
