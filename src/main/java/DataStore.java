import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * @author Achilles
 */
public class DataStore {
    private static final Map<String, ValueEntry> map = new ConcurrentHashMap<>();
    //SET方法需要能接受一个可选的过期时间
    public static void set(String key, byte[] value, long ttlMillis) {
        long expiryTimestamp;
        if (ttlMillis > 0) {
            // 计算绝对过期时间点
            expiryTimestamp = System.currentTimeMillis() + ttlMillis;
        } else {
            // -1 或 0 表示永不过期
            expiryTimestamp = -1;
        }
        map.put(key, new ValueEntry(value, expiryTimestamp));
    }

    // GET 方法中实现“被动删除”的核心逻辑
    public static byte[] get(String key) {
        ValueEntry entry = map.get(key);

        if (entry == null) {
            return null; // Key 不存在
        }

        // 核心：在访问时检查是否过期
        if (entry.isExpired()) {
            // 如果已过期，就从 map 中删除它，并返回 null
            map.remove(key);
            return null;
        }

        // 如果未过期，正常返回值
        return entry.value;
    }
}
