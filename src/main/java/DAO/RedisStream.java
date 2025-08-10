package DAO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Achilles
 * 用于表示整个Stream数据结构
 */
public class RedisStream {
    private final List<StreamEntry> entries=new ArrayList<>();

    //添加新条目到Stream
    public StreamEntryID add(StreamEntry entry) {
        // 验证逻辑已移至 DataStore，这里只负责添加
        entries.add(entry);
        return entry.id;
    }

    //获取最后一个ID
    public StreamEntryID getLastId(){
        if(entries.isEmpty()){
            return null;
        }
        return entries.get(entries.size()-1).id;
    }
}
