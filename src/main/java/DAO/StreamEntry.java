package DAO;

import java.util.Map;

/**
 * @author Achilles
 * 用于表示Stream中的一个条目
 */
public class StreamEntry {
    final StreamEntryID id;
    final Map<String,byte[]> fields;

    public StreamEntry(StreamEntryID id, Map<String, byte[]> fields) {
        this.id = id;
        this.fields = fields;
    }
}
