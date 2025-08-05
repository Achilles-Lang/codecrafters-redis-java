package DAO;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * @author Achilles
 */
public class DataStore {
    // 使用 Object 作为值，以存储 ValueEntry (字符串) 或 List<byte[]> (列表) 等
    private static final Map<String, Object> map = new ConcurrentHashMap<>();

    // --- 字符串操作 ---
    public static void setString(String key, ValueEntry value) {
        map.put(key, value);
    }

    public static ValueEntry getString(String key) {
        Object value = map.get(key);
        // 在获取时检查类型
        if (value instanceof ValueEntry) {
            ValueEntry entry = (ValueEntry) value;
            // 被动删除：如果过期了，就移除并返回 null
            if (entry.isExpired()) {
                map.remove(key);
                return null;
            }
            return entry;
        }
        // 如果 key 存在但不是字符串类型（比如是列表），也返回 null
        return null;
    }

    // --- 列表操作 ---
    /**
     * 将一个或多个值推入列表末尾。
     * 如果 key 不存在，则创建新列表。
     * @return 返回操作后列表的长度。如果 key 存在但不是列表，则返回 -1 (错误码)。
     */
    public static int rpush(String key, List<byte[]> valuesToPush) {
        Object value = map.get(key);
        List<byte[]> list;

        if (value == null) {
            // 如果 key 不存在，创建新列表
            list = new ArrayList<>();
            map.put(key, list);
        } else if (value instanceof List) {
            // 如果是列表，直接使用
            list = (List<byte[]>) value;
        } else {
            // 如果 key 存在但不是列表，返回错误码
            return -1;
        }

        list.addAll(valuesToPush);
        return list.size();
    }
}
