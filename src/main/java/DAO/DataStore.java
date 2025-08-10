package DAO;

import Config.WrongTypeException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Achilles
 */
public class DataStore {

    //新增一个全局锁对象
    private static final Object lock = new Object();


    // 使用 Object 作为值，以存储 ValueEntry (字符串) 或 List<byte[]> (列表) 等
    private static final Map<String, Object> map = new ConcurrentHashMap<>();

    // --- 字符串操作 ---
    public static void setString(String key, ValueEntry value) {
        synchronized (lock) {
            map.put(key, value);
        }
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
        synchronized (lock) {
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
            lock.notifyAll();
            return list.size();
        }

    }
    /**
     * 将一个或多个值推入列表头部。
     * @param key 列表的 key
     * @param valuesToPush 要添加的元素
     * @return 返回操作后列表的长度。如果 key 存在但不是列表，则返回 -1。
     */
    public static int lpush(String key, List<byte[]> valuesToPush) throws WrongTypeException {
        synchronized (lock) {
            Object existingValue = map.get(key);

            LinkedList<byte[]> list;

            if (existingValue == null) {
                // 如果 key 不存在，创建新 LinkedList
                list = new LinkedList<>();
                map.put(key, list);
            } else if (existingValue instanceof List) {
                // 如果已存在的是 ArrayList，为了效率创建一个新的 LinkedList
                list= (LinkedList<byte[]>) existingValue;
            } else {
                throw new WrongTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            // 遍历要插入的元素，逐个添加到列表头部
            // LPUSH a b c -> 列表最终是 [c, b, a, ...]
            // 所以我们按 a, b, c 的顺序，依次在索引 0 处插入
            for (byte[] value : valuesToPush) {
                // LinkedList.addFirst() 是 O(1) 操作，效率很高
                list.addFirst(value);
            }
            lock.notifyAll();
            return list.size();
        }

    }
    /**
     * 从列表左侧（头部）移除并返回指定数量的元素
     * @param key 列表的 key
     * @param count 要移除的元素数量
     * @return 被移除的元素列表。如果 key 不存在，返回 null。如果 key 存在但列表为空，返回空列表。
     * @throws WrongTypeException 如果 key 存在但不是列表类型。
     */
    public static List<byte[]> lpop(String key,int count) throws WrongTypeException {
        synchronized (lock) {
            Object value = map.get(key);

            // 情况 2: key 不存在，返回 null (代表 NIL)
            if (value == null) {
                return null;
            }

            // 情况 3: key 存在但不是列表，抛出异常
            if (!(value instanceof List)) {
                throw new WrongTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            @SuppressWarnings("unchecked")
            LinkedList<byte[]> list = (LinkedList<byte[]>) value;

            int actualCount = Math.min(list.size(), count);

            List<byte[]> poppedElements = new ArrayList<>(actualCount);
            for (int i = 0; i < actualCount; i++) {
                poppedElements.add(list.removeFirst());
            }

            // 情况 1: 列表不为空，移除并返回第一个元素
            // LinkedList.removeFirst() 是 O(1) 操作，效率很高
            return poppedElements;
        }

    }

    /**
     * 阻塞式地从列表左侧弹出一个元素，支持超时。
     * @param key 列表的 key
     * @param timeoutSeconds 超时时间（秒）。0 表示无限等待。
     * @return 弹出的元素。如果超时，返回 null。
     * @throws WrongTypeException 如果 key 存在但不是列表。
     * @throws InterruptedException 如果线程在等待时被中断。
     */
    public static byte[] blpop(String key, double timeoutSeconds) throws WrongTypeException, InterruptedException {
        synchronized (lock) {
            Object value = map.get(key);
            // 在进入循环前，先检查一次类型是否正确
            if (value != null && !(value instanceof List)) {
                throw new WrongTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            @SuppressWarnings("unchecked")
            LinkedList<byte[]> list = (LinkedList<byte[]>) value;

            // 如果列表已存在且非空，直接弹出并返回，无需阻塞
            if (list != null && !list.isEmpty()) {
                byte[] popped = list.removeFirst();
                if (list.isEmpty()) {
                    // 可选：如果列表空了，可以从 map 中移除，以节省内存
                    // map.remove(key);
                }
                return popped;
            }

            // --- 超时阻塞逻辑 ---
            long deadline = (timeoutSeconds > 0) ? (System.currentTimeMillis() + (long)(timeoutSeconds * 1000)) : 0;
            LinkedList<byte[]> getList=(LinkedList<byte[]>) map.get(key);
            while (list==null || list.isEmpty()) { // 使用无限循环，退出条件在内部处理
                // 重新获取 list，因为它的引用可能在等待期间改变
                value = map.get(key);
                if (value instanceof List) {
                    list = (LinkedList<byte[]>) value;
                    if (!list.isEmpty()) {
                        // 成功获取到元素，跳出循环
                        break;
                    }
                } else if (value != null) {
                    // key 被设置成了非 List 类型
                    throw new WrongTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
                }

                // --- 等待逻辑 ---
                if (timeoutSeconds > 0) {
                    long remainingTime = deadline - System.currentTimeMillis();
                    if (remainingTime <= 0) {
                        return null; // 时间到了，列表仍然是空的，超时返回 null
                    }
                    lock.wait(remainingTime);
                } else {
                    lock.wait(); // 无限期等待
                }
                Object valueAfterWait = map.get(key);
                if(valueAfterWait!=null&&!(valueAfterWait instanceof List)){
                    throw new WrongTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");

                }
                list=(LinkedList<byte[]>) valueAfterWait;
            }


            return list.removeFirst();
        }
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
     * 向 Stream 添加一个新条目，支持部分 ID 自动生成。
     * @param key Stream 的 key
     * @param reqTimestamp 请求的时间戳部分
     * @param reqSequence 请求的序列号部分, -1 代表自动生成
     * @param fields 新条目的键值对
     * @return 成功添加后返回条目的最终 ID
     * @throws Exception 如果发生错误
     */
    public static StreamEntryID xadd(String key, long reqTimestamp, int reqSequence,Map<String, byte[]> fields) throws Exception {
        synchronized (lock){
            Object value = map.get(key);
            RedisStream stream;

            if (value == null) {
                stream = new RedisStream();
                map.put(key, stream);
            } else if (value instanceof RedisStream) {
                stream = (RedisStream) value;
            } else {
                throw new WrongTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            StreamEntryID lastId = stream.getLastId();
            StreamEntryID finalId;

            if(reqSequence==-1){
                if(lastId!=null&&reqTimestamp<lastId.timestamp){
                    throw new Exception("The ID specified in XADD is equal or smaller than the target stream top item");
                }
                if(lastId!=null&&reqTimestamp==lastId.timestamp){
                    finalId=new StreamEntryID(reqTimestamp,lastId.sequence+1);
                }else {
                    int restartSequence=(reqTimestamp==0&&lastId==null)?1:0;
                    finalId=new StreamEntryID(reqTimestamp,restartSequence);
                }
            }else {
                finalId=new StreamEntryID(reqTimestamp,reqSequence);
            }

            StreamEntry newEntry=new StreamEntry(finalId,fields);

            return stream.add(newEntry);
        }

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
    /**
     * 获取指定 key 存储的值的类型。
     * @param key 要检查的 key
     * @return 代表类型的字符串 ("string", "list", "none")。
     */
    public static String getType(String key) {
        Object value = map.get(key);

        // 情况 1: key 不存在
        if (value == null) {
            return "none";
        }

        // 情况 2: key 存在，使用 instanceof 判断其具体类型
        if (value instanceof ValueEntry) {
            // 我们用 ValueEntry 来存储字符串及其元数据
            return "string";
        } else if (value instanceof List) {
            return "list";
        } else if (value instanceof Map) {
            return "hash";
        } else if (value instanceof Set) {
            return "set";
        } else if (value instanceof RedisStream) {
            return "stream";
        }

        return "unknown"; // 理论上不应该发生
    }
}
