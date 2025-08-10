package DAO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Achilles
 * 用于表示整个Stream数据结构
 */
public class RedisStream {
    private final List<StreamEntry> entries=new ArrayList<>();

    //添加新条目到Stream
    public StreamEntryID add(StreamEntry newEntry) throws Exception {
        // 验证 1：ID 不能是 0-0
        if (newEntry.id.timestamp == 0 && newEntry.id.sequence == 0) {
            throw new Exception("The ID specified in XADD must be greater than 0-0");
        }

        // 验证 2：新 ID 必须大于最后一个 ID
        if (!entries.isEmpty()) {
            StreamEntryID lastId = entries.get(entries.size() - 1).id;
            if (newEntry.id.compareTo(lastId) <= 0) {
                throw new Exception("The ID specified in XADD is equal or smaller than the target stream top item");
            }
        }

        // 验证通过，添加新条目
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
