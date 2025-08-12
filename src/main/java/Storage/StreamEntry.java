package Storage;

import java.util.Map;

/**
 * @author Achilles
 * 用于表示Stream中的一个条目
 */
public class StreamEntry {
    public final StreamEntryID id;
    public final Map<String,byte[]> fields;

    public StreamEntry(StreamEntryID id, Map<String, byte[]> fields) {
        this.id = id;
        this.fields = fields;
    }
}
