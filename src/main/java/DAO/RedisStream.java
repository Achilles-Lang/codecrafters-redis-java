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
        if(entry.id.timestamp==0&&entry.id.sequence==0){
            throw new Exception("The ID specified in XADD must be greater than 0-0");
        }

        if(!entries.isEmpty()){
           StreamEntryID lastId=entries.get(entries.size()-1).id;
           //新ID必须比最后一个ID大
           if(entry.id.compareTo(lastId)<=0){
               throw new Exception("The ID specified in XADD is equal or smaller than the target stream top item");
           }
       }
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
