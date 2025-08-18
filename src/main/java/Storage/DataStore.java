package Storage;

import Config.WrongTypeException;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Achilles
 * 单例，管理所有内存数据
 */
public class DataStore {
    private static final DataStore instance = new DataStore();

    private final Map<String, Object> map = new ConcurrentHashMap<>();

    private final ReplicationInfo replicationInfo =new ReplicationInfo();

    private DataStore() {
    }

    public static DataStore getInstance() {
        return instance;
    }

    public String getRole() {
        return this.replicationInfo.getRole();
    }

    public ReplicationInfo getReplicationInfo() {
        return this.replicationInfo;
    }

    public void setAsReplica(String masterHost,int masterPort) {
        this.replicationInfo.setRole("slave");
        this.replicationInfo.setMasterHost(masterHost);
        this.replicationInfo.setMasterPort(masterPort);
    }

    // --- 字符串操作 ---
    public synchronized void setString(String key, ValueEntry value) {
        map.put(key, value);
        this.notifyAll();
    }

    public synchronized ValueEntry getString(String key) {
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
    private LinkedList<byte[]> getOrCreateList(String key) throws WrongTypeException{
        Object value = map.get(key);
        if(value==null) {
            LinkedList<byte[]> newList = new LinkedList<>();
            map.put(key, newList);
            return newList;
        }
        if(!(value instanceof List)){
            throw new WrongTypeException("Operation against a key holding the wrong kind of value");
        }
        return (LinkedList<byte[]>) value;
    }

    // --- 列表操作 ---

    /**
     * 将一个或多个值推入列表末尾。
     * 如果 key 不存在，则创建新列表。
     *
     * @return 返回操作后列表的长度。如果 key 存在但不是列表，则返回 -1 (错误码)。
     */
    public synchronized int rpush(String key, List<byte[]> valuesToPush) throws WrongTypeException {
        List<byte[]> list = getOrCreateList(key);
        list.addAll(valuesToPush);
        this.notifyAll();
        return  list.size();
    }

    /**
     * 将一个或多个值推入列表头部。
     *
     * @param key          列表的 key
     * @param valuesToPush 要添加的元素
     * @return 返回操作后列表的长度。如果 key 存在但不是列表，则返回 -1。
     */
    public synchronized int lpush(String key, List<byte[]> valuesToPush) throws WrongTypeException {
        Object value = map.get(key);

        LinkedList<byte[]> list;

        if (value == null) {
            // 如果 key 不存在，创建新 LinkedList
            list = new LinkedList<>();
            map.put(key, list);
        } else if (value instanceof List) {
            // 如果已存在的是 ArrayList，为了效率创建一个新的 LinkedList
            list = (LinkedList<byte[]>) value;
        } else {
            throw new WrongTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        // 遍历要插入的元素，逐个添加到列表头部
        // LPUSH a b c -> 列表最终是 [c, b, a, ...]
        // 所以我们按 a, b, c 的顺序，依次在索引 0 处插入
        for (byte[] v : valuesToPush) {
            // LinkedList.addFirst() 是 O(1) 操作，效率很高
            list.addFirst(v);
        }
        this.notifyAll();
        return list.size();
    }

    /**
     * 从列表左侧（头部）移除并返回指定数量的元素
     *
     * @param key   列表的 key
     * @param count 要移除的元素数量
     * @return 被移除的元素列表。如果 key 不存在，返回 null。如果 key 存在但列表为空，返回空列表。
     * @throws WrongTypeException 如果 key 存在但不是列表类型。
     */
    public synchronized List<byte[]> lpop(String key, int count) throws WrongTypeException {
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

    /**
     * 阻塞式地从列表左侧弹出一个元素，支持超时。
     *
     * @param keys            列表的 key
     * @param timeoutSeconds 超时时间（秒）。0 表示无限等待。
     * @return 弹出的元素。如果超时，返回 null。
     * @throws WrongTypeException   如果 key 存在但不是列表。
     * @throws InterruptedException 如果线程在等待时被中断。
     */
    public synchronized Object[] blpop(List<byte[]> keys, double timeoutSeconds) throws WrongTypeException, InterruptedException {
        long deadLine = (timeoutSeconds > 0) ? (System.currentTimeMillis() + (long) (timeoutSeconds * 1000)) : 0;
        while (true) {
            for (byte[] keyBytes : keys) {
                String blpopKey = new String(keyBytes, StandardCharsets.UTF_8);
                Object value = map.get(blpopKey);
                if (value != null) {
                    if (!(value instanceof List)) {
                        throw new WrongTypeException("Operation against a key holding the wrong kind of value");
                    }
                    LinkedList<byte[]> list = (LinkedList<byte[]>) value;
                    if (!list.isEmpty()) {
                        return new Object[]{keyBytes, list.removeFirst()};
                    }
                }
            }

            long remainingTime;
            if(timeoutSeconds>0){
                remainingTime = deadLine-System.currentTimeMillis();
                if(remainingTime<=0){
                    return null;
                }
            }else {
                remainingTime = 0;
            }
            this.wait(remainingTime);
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
    public synchronized List<byte[]> lrange(String key, int start, int end) throws WrongTypeException {
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
    public synchronized StreamEntryID xadd(String key, long reqTimestamp, int reqSequence, Map<String, byte[]> fields) throws Exception {
        System.out.println("[DEBUG] DataStore: Entering xadd method.");

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

        if (reqTimestamp == -1 && reqSequence == -1) {
            // --- 情况3: 完全自动生成 ID ("*") ---
            long newTimestamp = System.currentTimeMillis();
            int newSequence = 0;

            // **关键修复**: 使用正确的字段名 "timeStamp"
            if (lastId != null && newTimestamp <= lastId.timestamp) {
                newTimestamp = lastId.timestamp;
                newSequence = lastId.sequence + 1;
            }
            finalId = new StreamEntryID(newTimestamp, newSequence);

        } else if (reqSequence == -1) {
            // --- 情况2: 部分自动生成 ID ("timestamp-*") ---
            long finalTimestamp = reqTimestamp;
            int finalSequence;

            // **关键修复**: 使用正确的字段名 "timeStamp"
            if (lastId != null && finalTimestamp < lastId.timestamp) {
                throw new Exception("The ID specified in XADD is equal or smaller than the target stream top item");
            }
            if (lastId != null && finalTimestamp == lastId.timestamp) {
                finalSequence = lastId.sequence + 1;
            } else {
                finalSequence = (finalTimestamp == 0 && lastId == null) ? 1 : 0;
            }
            finalId = new StreamEntryID(finalTimestamp, finalSequence);
        } else {
            // --- 情况1：用户提供了完整的 ID ---
            finalId = new StreamEntryID(reqTimestamp, reqSequence);
        }
        StreamEntryID newId=stream.add(finalId, fields);
        this.notifyAll();

        // 调用 RedisStream.add 进行最终验证和添加
        return newId;
    }

    /**
     * 获取列表的长度。
     * @param key 列表的 key
     * @return 列表的长度。如果 key 不存在，返回 0。
     * @throws WrongTypeException 如果 key 存在但不是列表类型。
     */
    public synchronized int llen(String key) throws WrongTypeException {
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
    public synchronized String getType(String key) {
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
    /**
     * 查询 Stream 中在指定 ID 范围内的条目。
     * @param key Stream 的 key
     * @param startId 起始 ID (包含)
     * @param endId 结束 ID (包含)
     * @return 包含符合条件条目的列表。如果 key 不存在，返回空列表。
     * @throws WrongTypeException 如果 key 对应的值不是 Stream。
     */
    public synchronized List<StreamEntry> xrange(String key, StreamEntryID startId, StreamEntryID endId) throws WrongTypeException {
            Object value = map.get(key);

            if (value == null) {
                return new ArrayList<>();
            }

            if (!(value instanceof RedisStream)) {
                throw new WrongTypeException("Operation against a key holding the wrong kind of value");
            }

            RedisStream stream = (RedisStream) value;
            List<StreamEntry> results = new ArrayList<>();

            // 因为 Stream 条目是按 ID 排序的，直接遍历即可
            for (StreamEntry entry : stream.getEntries()) {
                // 检查 entry.id 是否在 [startId, endId] 区间内
                if (entry.id.compareTo(startId) >= 0 && entry.id.compareTo(endId) <= 0) {
                    results.add(entry);
                }
            }
            return results;
    }
    /**
     * 从一个或多个 Stream 中读取ID大于指定ID的条目。
     * @param streamsToRead 一个 Map，key 是 Stream 的 key，value 是起始 ID (不包含)。
     * @return 一个 Map，key 是 Stream 的 key，value 是所有符合条件的条目列表。
     * @param timeoutMillis 阻塞时长，单位毫秒。
     * @throws WrongTypeException 如果某个 key 对应的值不是 Stream。
     */
    public synchronized Map<String, List<StreamEntry>> xread(Map<String, StreamEntryID> streamsToRead,long timeoutMillis) throws WrongTypeException, InterruptedException {
        long deadline = (timeoutMillis > 0) ? (System.currentTimeMillis() + timeoutMillis) : 0;

        // 创建一个新的 Map 来存放解析后的、具体的起始 ID
        Map<String, StreamEntryID> resolvedStreamsToRead = new LinkedHashMap<>();
        for (Map.Entry<String, StreamEntryID> query : streamsToRead.entrySet()) {
            String key = query.getKey();
            StreamEntryID startId = query.getValue();

            // 检查是否是我们为 "$" 设置的占位符 (例如 new StreamEntryID(-1, -1))
            if (startId.timestamp == -1 && startId.sequence == -1) {
                Object value = map.get(key);
                StreamEntryID lastId = null;
                if (value instanceof RedisStream) {
                    lastId = ((RedisStream) value).getLastId();
                }
                // 如果流为空或不存在，则从 0-0 开始查；否则从最后一个ID开始查
                resolvedStreamsToRead.put(key, (lastId != null) ? lastId : new StreamEntryID(0, 0));
            } else {
                resolvedStreamsToRead.put(key, startId);
            }
        }


        while (true) {
            // 1. 先执行一次非阻塞的查询
            Map<String, List<StreamEntry>> result = queryStreams(resolvedStreamsToRead);

            // 2. 如果有结果，或者这是一个非阻塞调用，立即返回
            if (!result.isEmpty() || timeoutMillis < 0) {
                return result;
            }

            // 3. 如果没结果且需要阻塞，则计算剩余时间并等待
            if (timeoutMillis == 0) {
                this.wait();
            }else {
                long remainingTime = deadline - System.currentTimeMillis();
                if(remainingTime <= 0){
                    return new LinkedHashMap<>();
                }
                this.wait(remainingTime);
            }
        }
    }
    /**
     * 辅助方法，执行一次实际的、非阻塞的流查询。
     */
    private Map<String, List<StreamEntry>> queryStreams(Map<String, StreamEntryID> streamsToRead) throws WrongTypeException {
        Map<String, List<StreamEntry>> result = new LinkedHashMap<>();
        for (Map.Entry<String, StreamEntryID> query : streamsToRead.entrySet()) {
            String key = query.getKey();
            StreamEntryID startId = query.getValue();
            Object value = map.get(key);
            if (value == null) {
                continue;
            }
            if (!(value instanceof RedisStream)) {
                throw new WrongTypeException("Operation against a key holding the wrong kind of value");
            }
            RedisStream stream = (RedisStream) value;
            List<StreamEntry> newEntries = new ArrayList<>();
            for (StreamEntry entry : stream.getEntries()) {
                if (entry.id.compareTo(startId) > 0) {
                    newEntries.add(entry);
                }
            }
            if (!newEntries.isEmpty()) {
                result.put(key, newEntries);
            }
        }
        return result;
    }
}
