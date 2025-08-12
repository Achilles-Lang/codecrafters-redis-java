package Storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Achilles
 * 数据模型：Stream
 */
public class RedisStream {
    private final List<StreamEntry> entries=new ArrayList<>();

    //添加新条目到Stream
    public StreamEntryID add(StreamEntryID id, Map<String, byte[]> fields) throws Exception {
        // 验证 1：ID 不能是 0-0
        if (id.timestamp == 0 && id.sequence == 0) {
            throw new Exception("The ID specified in XADD must be greater than 0-0");
        }

        // 验证 2：新 ID 必须大于流中最后一个 ID
        if (!entries.isEmpty()) {
            StreamEntryID lastId = this.getLastId(); // 使用 this.getLastId()
            if (id.compareTo(lastId) <= 0) {
                throw new Exception("The ID specified in XADD is equal or smaller than the target stream top item");
            }
        }

        // 验证通过，创建并添加新条目
        StreamEntry newEntry = new StreamEntry(id, fields);
        entries.add(newEntry);
        return newEntry.id;
    }

    //获取最后一个ID
    public StreamEntryID getLastId(){
        if(entries.isEmpty()){
            return null;
        }
        return entries.get(entries.size()-1).id;
    }
}
