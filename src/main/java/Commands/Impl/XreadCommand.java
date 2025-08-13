package Commands.Impl;

import Commands.Command;
import Config.WrongTypeException;
import Storage.DataStore;
import Storage.StreamEntry;
import Storage.StreamEntryID;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Achilles
 */
public class XreadCommand implements Command {
    @Override
    public Object execute(List<byte[]> args) {
// XREAD streams <key> <id>
        if (args.size() != 3) {
            return new Exception("wrong number of arguments for 'xread' command");
        }
        try {
            String streamsKeyword = new String(args.get(0), StandardCharsets.UTF_8);
            if (!"streams".equalsIgnoreCase(streamsKeyword)) {
                return new Exception("syntax error");
            }

            String key = new String(args.get(1), StandardCharsets.UTF_8);
            String idStr = new String(args.get(2), StandardCharsets.UTF_8);
            StreamEntryID startId = parseId(idStr);

            // 1. 准备给 DataStore 的参数
            Map<String, StreamEntryID> streamsToRead = new HashMap<>();
            streamsToRead.put(key, startId);

            // 2. 调用 DataStore 获取原始数据
            Map<String, List<StreamEntry>> resultData = DataStore.getInstance().xread(streamsToRead);

            // 3. 将 DataStore 返回的结果转换为 RESP 嵌套数组格式
            if (resultData.isEmpty()) {
                return null; // 如果没有新条目，返回 NIL
            }

            List<Object> response = new ArrayList<>();
            for (Map.Entry<String, List<StreamEntry>> streamResult : resultData.entrySet()) {
                // 外层数组 1: 代表一个 Stream 的结果 [key, [[id, [f,v,...]], ...]]
                List<Object> singleStreamResponse = new ArrayList<>();
                singleStreamResponse.add(streamResult.getKey().getBytes());

                // 中层数组: 代表该 Stream 所有的条目 [[id, [f,v,...]], [id, [f,v,...]]]
                List<List<Object>> entriesResponse = new ArrayList<>();
                for (StreamEntry entry : streamResult.getValue()) {
                    // 内层数组 1: 代表一个条目 [id, [f,v,...]]
                    List<Object> entryResponse = new ArrayList<>();
                    entryResponse.add(entry.id.toString().getBytes());

                    // 内层数组 2: 代表条目中的键值对 [f1, v1, f2, v2, ...]
                    List<byte[]> fieldsResponse = new ArrayList<>();
                    for (Map.Entry<String, byte[]> field : entry.fields.entrySet()) {
                        fieldsResponse.add(field.getKey().getBytes());
                        fieldsResponse.add(field.getValue());
                    }
                    entryResponse.add(fieldsResponse);
                    entriesResponse.add(entryResponse);
                }
                singleStreamResponse.add(entriesResponse);
                response.add(singleStreamResponse);
            }
            return response;

        } catch (WrongTypeException e) {
            return e;
        } catch (Exception e) {
            return new Exception("Invalid stream ID specified as XREAD argument");
        }
    }
    /**
     * 辅助方法，用于解析Stream ID。
     */
    private StreamEntryID parseId(String idStr){
        String[] parts = idStr.split("-");
        long timestamp = Long.parseLong(parts[0]);
        int sequence = Integer.parseInt(parts[1]);
        return new StreamEntryID(timestamp, sequence);
    }
}
