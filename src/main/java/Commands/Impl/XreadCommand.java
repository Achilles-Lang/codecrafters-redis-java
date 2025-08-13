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
        try {
            int streamsIndex=-1;
            for(int i=0;i<args.size();i++){
                if("streams".equalsIgnoreCase(new String(args.get(i),StandardCharsets.UTF_8))){
                    streamsIndex=i;
                    break;
                }
            }

            if(streamsIndex==-1){
                return new Exception("Syntax error in XREAD command. Missing STREAMS keyword.");
            }

            int totalKeysAndIds = args.size() - (streamsIndex + 1);
            if (totalKeysAndIds % 2 != 0 || totalKeysAndIds == 0) {
                return new Exception("wrong number of arguments for 'xread' command");
            }

            int numKeys = totalKeysAndIds / 2;
            List<byte[]> keys = args.subList(streamsIndex + 1, streamsIndex + 1 + numKeys);
            List<byte[]> ids = args.subList(streamsIndex + 1 + numKeys, args.size());

            Map<String, StreamEntryID> streamsToRead = new HashMap<>();
            for (int i = 0; i < numKeys; i++) {
                String key = new String(keys.get(i), StandardCharsets.UTF_8);
                String idStr = new String(ids.get(i), StandardCharsets.UTF_8);
                // 复用我们之前为 XRANGE 编写的 ID 解析器
                streamsToRead.put(key, XrangeCommand.parseId(idStr, true));
            }

            Map<String,List<StreamEntry>> resultData=DataStore.getInstance().xread(streamsToRead);

            if(resultData.isEmpty()){
                return null;
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
    private  StreamEntryID parseId(String idStr){
        String[] parts = idStr.split("-");
        long timestamp = Long.parseLong(parts[0]);
        int sequence = Integer.parseInt(parts[1]);
        return new StreamEntryID(timestamp, sequence);
    }
}
