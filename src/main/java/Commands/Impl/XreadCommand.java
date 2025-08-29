package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Config.WrongTypeException;
import Storage.DataStore;
import Storage.StreamEntry;
import Storage.StreamEntryID;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author Achilles
 */
public class XreadCommand implements Command {

    private static final StreamEntryID LATEST_ID_PLACEHOLDER = new StreamEntryID(-1, -1);

    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        // XREAD streams <key> <id>
        try {
            //默认，非阻塞
            long timeoutMillis = -1;
            int streamsIndex = -1;

            //解析BLOCK参数
            if (args.size() > 1 && "block".equalsIgnoreCase(new String(args.get(0)))) {
                if (args.size() < 4) {
                    return new Exception("wrong number of arguments for 'xread' command with BLOCK");
                }
                timeoutMillis = Long.parseLong(new String(args.get(1)));
                //找到stream关键字在block之后的位置
                for (int i = 2; i < args.size(); i++) {
                    if ("streams".equalsIgnoreCase(new String(args.get(i)))) {
                        streamsIndex = i;
                        break;
                    }
                }
            } else {
                //非阻塞情况
                for (int i = 0; i < args.size(); i++) {
                    if ("streams".equalsIgnoreCase(new String(args.get(i)))) {
                        streamsIndex = i;
                        break;
                    }
                }
            }
            //验证和解析stream <keys...> <ids...>部分
            if (streamsIndex == -1) {
                return new Exception("Syntax error in XREAD command. Missing STREAMS keyword.");
            }
            int numKeys = (args.size() - 1 - streamsIndex) / 2;
            if ((args.size() - 1 - streamsIndex) % 2 != 0 || numKeys == 0) {
                return new Exception("Unbalanced XREAD list of streams: keys and IDs must match.");
            }

            Map<String, StreamEntryID> streamsToRead = new LinkedHashMap<>();
            for (int i = 0; i < numKeys; i++) {
                String key = new String(args.get(streamsIndex + 1 + i));
                String idStr = new String(args.get(streamsIndex + 1 + numKeys + i));

                if("$".equals(idStr)){
                    streamsToRead.put(key, LATEST_ID_PLACEHOLDER);
                }else{
                    streamsToRead.put(key, XrangeCommand.parseId(idStr,true));
                }
            }



            //调用Datastore方法
            Map<String, List<StreamEntry>> resultData = DataStore.getInstance().xread(streamsToRead,timeoutMillis);

            if (resultData.isEmpty()||resultData==null) {
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
