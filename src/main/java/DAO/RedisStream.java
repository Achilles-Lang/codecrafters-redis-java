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
    public StreamEntryID add(StreamEntry entry) throws Exception {
       if(!entries.isEmpty()){
           StreamEntryID lastId=entries.get(entries.size()-1).id;
           //新ID必须比最后一个ID大
           if(entry.id.compareTo(lastId)<=0){
           throw  new Exception("XADD 中指定的 ID 等于或小于目标流顶部项目");
           }
       }
       entries.add(entry);
       return entry.id;
    }
}
