package DAO;

import Config.WrongTypeException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
            list = new LinkedList<>();
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
    /**
     * 将一个或多个值推入列表头部。
     * @param key 列表的 key
     * @param valuesToPush 要添加的元素
     * @return 返回操作后列表的长度。如果 key 存在但不是列表，则返回 -1。
     */
    public static int lpush(String key, List<byte[]> valuesToPush) {
        Object existingValue = map.get(key);

        LinkedList<byte[]> list;

        if (existingValue == null) {
            // 如果 key 不存在，创建新 LinkedList
            list = new LinkedList<>();
            map.put(key, list);
        } else if (existingValue instanceof List) {
            // 如果已存在的是 ArrayList，为了效率创建一个新的 LinkedList
            if (!(existingValue instanceof LinkedList)) {
                list = new LinkedList<>( (List<byte[]>) existingValue );
                map.put(key, list);
            } else {
                list = (LinkedList<byte[]>) existingValue;
            }
        } else {
            // 如果 key 存在但不是列表，返回错误码
            return -1;
        }

        // 遍历要插入的元素，逐个添加到列表头部
        // LPUSH a b c -> 列表最终是 [c, b, a, ...]
        // 所以我们按 a, b, c 的顺序，依次在索引 0 处插入
        for (byte[] value : valuesToPush) {
            // LinkedList.addFirst() 是 O(1) 操作，效率很高
            list.addFirst(value);
        }

        return list.size();
    }


    /**
     * 获取列表在指定范围内的元素。
     * @param key 列表的 key
     * @param start 起始索引
     * @param end 结束索引
     * @return 包含范围内元素的列表。如果 key 不存在，返回空列表。
     * @throws WrongTypeException 如果 key 对应的值不是列表。
     */
    public static List<byte[]> lrange(String key, int start, int end) throws WrongTypeException {
        Object value = map.get(key);

        // 1. 如果 key 不存在，根据 Redis 规范返回一个空列表
        if (value == null) {
            return new ArrayList<>();
        }

        // 2. 如果 key 存在但不是列表，抛出类型错误异常
        if (!(value instanceof List)) {
            throw new WrongTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        @SuppressWarnings("unchecked")
        List<byte[]> list = (List<byte[]>) value;
        int size = list.size();

        //负数索引转换为正数索引
        if (start < 0) {
            start = size + start;
        }
        if (end < 0) {
            end = size + end;
        }

        //索引越界情况
        if(start < 0){
            start = 0;
        }
        if (end >= size) {
            end = size - 1;
        }

        //如果修正后 start > end，说明范围无效，返回空列表
        if (start > end) {
            return new ArrayList<>();
        }

        // 5. 使用 subList 安全地获取子列表
        // 注意：Java 的 subList 的 toIndex 是不包含的 (exclusive)，
        // 而 LRANGE 的 end 是包含的 (inclusive)，所以我们需要 +1。
        return new ArrayList<>(list.subList(start, end + 1));
    }

    /**
     * 获取列表的长度。
     * @param key 列表的 key
     * @return 列表的长度。如果 key 不存在，返回 0。
     * @throws WrongTypeException 如果 key 存在但不是列表类型。
     */
    public static int llen(String key) throws WrongTypeException {
        Object value = map.get(key);

        // 情况 2: key 不存在，返回 0
        if (value == null) {
            return 0;
        }

        // 情况 3: key 存在但不是列表，抛出异常
        if (!(value instanceof List)) {
            throw new WrongTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        // 情况 1: key 是列表，返回其大小
        @SuppressWarnings("unchecked")
        List<byte[]> list = (List<byte[]>) value;
        return list.size();
    }
}
