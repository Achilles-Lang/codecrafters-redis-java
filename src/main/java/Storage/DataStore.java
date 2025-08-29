package Storage;

import Config.WrongTypeException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Achilles
 * 单例，管理所有内存数据
 */
public class DataStore {
    private static final DataStore instance = new DataStore();

    private final Map<String, Object> map = new ConcurrentHashMap<>();

    private final ReplicationInfo replicationInfo =new ReplicationInfo();

    private final List<OutputStream> replicas=new CopyOnWriteArrayList<>() ;

    private long masterWriteOffset=0L;

    private final Queue<AckCallback> ackCallbacks = new ConcurrentLinkedQueue<>();

    // This is no longer needed with the simplified wait/notify mechanism
    // private final Map<String,Queue<Object>> blpopWaitingQueues = new ConcurrentHashMap<>();

    private String rdbDir;

    private String rdbFileName;
    // 用于存储频道和订阅者列表的 Map
    private final Map<String,List<OutputStream>> subscriptions=new ConcurrentHashMap<>();
    // 用于追踪每个客户端订阅了那些频道
    private final Map<OutputStream, Set<String>> clientSubscriptions = new ConcurrentHashMap<>();

    private long replicaOffset=0L;

    public synchronized long getReplicaOffset() {
        return replicaOffset;
    }

    public synchronized void incrementReplicaOffset(long offset){
        this.replicaOffset+=offset;
    }

    public void setRdbConfig(String dir,String fileName){
        this.rdbDir=dir;
        this.rdbFileName=fileName;
    }

    public String getRdbDir(){
        return this.rdbDir;
    }

    public String getRdbFileName(){
        return this.rdbFileName;
    }

    public synchronized int getReplicaCount() {
        return this.replicas.size();
    }

    public synchronized Map<OutputStream, Set<String>> getClientSubscriptions() {
        return this.clientSubscriptions;
    }

    @FunctionalInterface
    public interface AckCallback {
        void onAckReceived(long offset);
    }

    public void registerAckCallback(AckCallback callback){
        ackCallbacks.add(callback);
    }

    public void removeAckCallback(AckCallback callback){
        ackCallbacks.remove(callback);
    }

    public void processAck(long offset){
        for(AckCallback callback:ackCallbacks){
            callback.onAckReceived(offset);
        }
    }

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

    public synchronized void  addToMasterOffset(long offset){
        this.masterWriteOffset+=offset;
    }

    public synchronized  long getMasterOffset(){
        return this.masterWriteOffset;
    }

    // --- 字符串操作 ---
    public synchronized void setString(String key, ValueEntry value) {
        map.put(key, value);
        // Wake up any threads waiting in BLPOP or XREAD
        this.notify();
    }

    public synchronized ValueEntry getString(String key) {
        Object value = map.get(key);
        if (value instanceof ValueEntry) {
            ValueEntry entry = (ValueEntry) value;
            if (entry.isExpired()) {
                map.remove(key);
                return null;
            }
            return entry;
        }
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
        if(!(value instanceof LinkedList)){
            LinkedList<byte[]> newList = new LinkedList<>((List<byte[]>) value);
            map.put(key, newList);
            return newList;
        }
        return (LinkedList<byte[]>) value;
    }

    // --- 列表操作 ---
    public synchronized int rpush(String key, List<byte[]> valuesToPush) throws WrongTypeException {
        List<byte[]> list = getOrCreateList(key);
        list.addAll(valuesToPush);

        // Wake up any threads waiting in BLPOP
        this.notify();
        return list.size();
    }

    public synchronized int lpush(String key, List<byte[]> valuesToPush) throws WrongTypeException {
        LinkedList<byte[]> list = getOrCreateList(key);
        for (byte[] v : valuesToPush) {
            list.addFirst(v);
        }
        // Wake up any threads waiting in BLPOP
        this.notify();
        return list.size();
    }

    public synchronized List<byte[]> lpop(String key, int count) throws WrongTypeException {
        Object value = map.get(key);
        if (value == null) {
            return null;
        }
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
        return poppedElements;
    }

    /**
     * [CORRECTED] 阻塞式地从列表左侧弹出一个元素，支持超时。
     * This version uses this.wait() and this.notifyAll() to prevent race conditions.
     */
    public synchronized Object[] blpop(List<byte[]> keys, double timeoutSeconds) throws WrongTypeException, InterruptedException {
        long deadline = (timeoutSeconds > 0) ? (System.currentTimeMillis() + (long)(timeoutSeconds * 1000)) : 0;

        // Loop to handle spurious wakeups and check for data
        while (true) {
            // 1. First, check for available data without blocking
            for (byte[] keyBytes : keys) {
                String key = new String(keyBytes, StandardCharsets.UTF_8);
                Object value = map.get(key);
                if (value instanceof List && !((List<?>) value).isEmpty()) {
                    // Data found, pop it and return
                    @SuppressWarnings("unchecked")
                    LinkedList<byte[]> list = (LinkedList<byte[]>) value;
                    return new Object[]{keyBytes, list.removeFirst()};
                }
            }

            // 2. If no data, calculate remaining time and decide whether to wait or timeout
            long waitTime = 0;
            if (timeoutSeconds > 0) {
                waitTime = deadline - System.currentTimeMillis();
                if (waitTime <= 0) {
                    return null; // Timeout
                }
            }

            // 3. Wait for a notification from a data-producing command (like RPUSH)
            // If timeout is 0, wait forever.
            this.wait(waitTime);
        }
    }


    public synchronized List<byte[]> lrange(String key, int start, int end) throws WrongTypeException {
        Object value = map.get(key);

        if (value == null) {
            return new ArrayList<>();
        }

        if (!(value instanceof List)) {
            throw new WrongTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        @SuppressWarnings("unchecked")
        List<byte[]> list = (List<byte[]>) value;
        int size = list.size();

        if (start < 0) {
            start = size + start;
        }
        if (end < 0) {
            end = size + end;
        }
        if(start < 0) {
            start = 0;
        }
        if (end >= size) {
            end = size - 1;
        }
        if (start > end) {
            return new ArrayList<>();
        }

        return new ArrayList<>(list.subList(start, end + 1));
    }

    public synchronized StreamEntryID xadd(String key, long reqTimestamp, int reqSequence, Map<String, byte[]> fields) throws Exception {
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
            long newTimestamp = System.currentTimeMillis();
            int newSequence = 0;
            if (lastId != null && newTimestamp <= lastId.timestamp) {
                newTimestamp = lastId.timestamp;
                newSequence = lastId.sequence + 1;
            }
            finalId = new StreamEntryID(newTimestamp, newSequence);
        } else if (reqSequence == -1) {
            long finalTimestamp = reqTimestamp;
            int finalSequence;
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
            finalId = new StreamEntryID(reqTimestamp, reqSequence);
        }
        StreamEntryID newId = stream.add(finalId, fields);
        // Wake up any threads waiting in XREAD
        this.notify();
        return newId;
    }

    public synchronized int llen(String key) throws WrongTypeException {
        Object value = map.get(key);
        if (value == null) {
            return 0;
        }
        if (!(value instanceof List)) {
            throw new WrongTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        @SuppressWarnings("unchecked")
        List<byte[]> list = (List<byte[]>) value;
        return list.size();
    }

    public synchronized String getType(String key) {
        Object value = map.get(key);
        if (value == null) {
            return "none";
        }
        if (value instanceof ValueEntry) {
            return "string";
        }
        if (value instanceof List) {
            return "list";
        }
        if (value instanceof Map) {
            return "hash";
        }
        if (value instanceof Set) {
            return "set";
        }
        if (value instanceof RedisStream) {
            return "stream";
        }
        return "unknown";
    }

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
        for (StreamEntry entry : stream.getEntries()) {
            if (entry.id.compareTo(startId) >= 0 && entry.id.compareTo(endId) <= 0) {
                results.add(entry);
            }
        }
        return results;
    }

    public synchronized Map<String, List<StreamEntry>> xread(Map<String, StreamEntryID> streamsToRead,long timeoutMillis) throws WrongTypeException, InterruptedException {
        long deadline = (timeoutMillis >= 0) ? (System.currentTimeMillis() + timeoutMillis) : 0;

        Map<String, StreamEntryID> resolvedStreamsToRead = new LinkedHashMap<>();
        for (Map.Entry<String, StreamEntryID> query : streamsToRead.entrySet()) {
            String key = query.getKey();
            StreamEntryID startId = query.getValue();
            if (startId.timestamp == -1 && startId.sequence == -1) {
                Object value = map.get(key);
                StreamEntryID lastId = (value instanceof RedisStream) ? ((RedisStream) value).getLastId() : null;
                resolvedStreamsToRead.put(key, (lastId != null) ? lastId : new StreamEntryID(0, 0));
            } else {
                resolvedStreamsToRead.put(key, startId);
            }
        }

        while (true) {
            Map<String, List<StreamEntry>> result = queryStreams(resolvedStreamsToRead);
            if (!result.isEmpty() || timeoutMillis < 0) {
                return result;
            }
            if (timeoutMillis == 0) {
                this.wait();
            } else {
                long remainingTime = deadline - System.currentTimeMillis();
                if(remainingTime <= 0) {
                    return new LinkedHashMap<>();
                }
                this.wait(remainingTime);
            }
        }
    }

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

    public synchronized void addReplica(OutputStream replicaOutputStream) {
        replicas.add(replicaOutputStream);
    }

    public synchronized List<OutputStream> getReplicas() {
        return new ArrayList<>(replicas);
    }

    public synchronized void broadcastToReplicas(String... commandParts) {
        if (replicas.isEmpty()) {
            return;
        }
        byte[] respCommand = encodeCommand(commandParts);
        Iterator<OutputStream> iterator = replicas.iterator();
        while (iterator.hasNext()) {
            OutputStream replicaOs = iterator.next();
            try {
                replicaOs.write(respCommand);
                replicaOs.flush();
            } catch (IOException e) {
                System.out.println("Replica connection lost. Removing from list.");
                iterator.remove();
            }
        }
    }

    private byte[] encodeCommand(String... parts) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(parts.length).append("\r\n");
        for (String part : parts) {
            sb.append("$").append(part.getBytes(StandardCharsets.UTF_8).length).append("\r\n");
            sb.append(part).append("\r\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    public synchronized void propagateCommand(List<byte[]> commandParts) {
        long commandSize = calculateAndGetCommandSize(commandParts);
        this.masterWriteOffset += commandSize;
        byte[] respCommand = encodeCommandFromParts(commandParts);
        if (replicas.isEmpty()) {
            return;
        }
        Iterator<OutputStream> iterator = replicas.iterator();
        while (iterator.hasNext()) {
            OutputStream replicaOs = iterator.next();
            try {
                replicaOs.write(respCommand);
                replicaOs.flush();
            } catch (IOException e) {
                System.out.println("Replica connection lost. Removing from list.");
                iterator.remove();
            }
        }
    }

    private byte[] encodeCommandFromParts(List<byte[]> parts) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            baos.write(("*" + parts.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (byte[] part : parts) {
                baos.write(("$" + part.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                baos.write(part);
                baos.write("\r\n".getBytes(StandardCharsets.UTF_8));
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long calculateAndGetCommandSize(List<byte[]> commandParts) {
        long totalSize = 0;
        totalSize += ("*" + commandParts.size() + "\r\n").getBytes(StandardCharsets.UTF_8).length;
        for (byte[] part : commandParts) {
            totalSize += ("$" + part.length + "\r\n").getBytes(StandardCharsets.UTF_8).length;
            totalSize += part.length;
            totalSize += "\r\n".getBytes(StandardCharsets.UTF_8).length;
        }
        return totalSize;
    }

    public synchronized List<String> getAllKeys(){
        return new ArrayList<>(map.keySet());
    }

    public synchronized void subscribe(String channel, OutputStream clientStream) {
        subscriptions.computeIfAbsent(channel, k -> new CopyOnWriteArrayList<>()).add(clientStream);
        clientSubscriptions.computeIfAbsent(clientStream, k -> ConcurrentHashMap.newKeySet()).add(channel);
    }

    public synchronized int getSubscriptionCountForClient(OutputStream clientStream) {
        Set<String> channels = clientSubscriptions.get(clientStream);
        return (channels == null) ? 0 : channels.size();
    }

    public synchronized void unsubscribeClient(OutputStream clientStream) {
        Set<String> subscribedChannels = clientSubscriptions.remove(clientStream);
        if (subscribedChannels != null) {
            for (String channel : subscribedChannels) {
                List<OutputStream> subscribers = subscriptions.get(channel);
                if (subscribers != null) {
                    subscribers.remove(clientStream);
                }
            }
        }
    }

    public synchronized int publishMessage(String channel, byte[] message) {
        List<OutputStream> subscribers = subscriptions.get(channel);
        if (subscribers == null || subscribers.isEmpty()) {
            return 0;
        }
        byte[] respMessage = buildMessagePayload(channel.getBytes(StandardCharsets.UTF_8), message);
        int deliveredCount = 0;
        Iterator<OutputStream> iterator = subscribers.iterator();
        while (iterator.hasNext()) {
            OutputStream subscriberStream = iterator.next();
            try {
                subscriberStream.write(respMessage);
                subscriberStream.flush();
                deliveredCount++;
            } catch (IOException e) {
                System.out.println("Subscriber connection lost. Removing from list.");
                iterator.remove();
                unsubscribeClient(subscriberStream);
            }
        }
        return deliveredCount;
    }

    private byte[] buildMessagePayload(byte[] channel, byte[] message) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            baos.write("*3\r\n".getBytes(StandardCharsets.UTF_8));
            baos.write("$7\r\nmessage\r\n".getBytes(StandardCharsets.UTF_8));
            baos.write(("$" + channel.length + "\r\n").getBytes(StandardCharsets.UTF_8));
            baos.write(channel);
            baos.write("\r\n".getBytes(StandardCharsets.UTF_8));
            baos.write(("$" + message.length + "\r\n").getBytes(StandardCharsets.UTF_8));
            baos.write(message);
            baos.write("\r\n".getBytes(StandardCharsets.UTF_8));
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized int getSubscriberCount(String channelName) {
        List<OutputStream> subscribers = subscriptions.get(channelName);
        return (subscribers == null) ? 0 : subscribers.size();
    }
    /**
     * **新增**: 将一个客户端从指定频道退订。
     * @param channel 要退订的频道
     * @param clientStream 退订的客户端的输出流
     */
    public synchronized void unsubscribe(String channel, OutputStream clientStream) {
        // 1. 从 "客户端 -> 频道集合" 的关系中移除
        Set<String> subscribedChannels = clientSubscriptions.get(clientStream);
        if (subscribedChannels != null) {
            subscribedChannels.remove(channel);
        }

        // 2. 从 "频道 -> 客户端列表" 的关系中移除
        List<OutputStream> subscribers = subscriptions.get(channel);
        if (subscribers != null) {
            subscribers.remove(clientStream);
            // 如果一个频道没有任何订阅者了，可以从Map中移除以节省内存
            if (subscribers.isEmpty()) {
                subscriptions.remove(channel);
            }
        }
    }

    /**
     * **新增 (可选但推荐)**: 从所有频道退订。
     * @param clientStream 要退订的客户端
     */
    public synchronized void unsubscribeFromAll(OutputStream clientStream) {
        // 1. 获取该客户端订阅的所有频道
        Set<String> channels = clientSubscriptions.get(clientStream);
        if (channels != null && !channels.isEmpty()) {
            // 2. 遍历它订阅过的所有频道，并从中移除该客户端
            // 创建一个副本以避免在遍历时修改集合
            for (String channel : new ArrayList<>(channels)) {
                unsubscribe(channel, clientStream);
            }
        }
    }
    /**
     * 向指定 key 的有序集合中添加一个或多个成员。
     * @param key 有序集合的 key
     * @param scoresAndMembers 一个列表，格式为 [score1, member1, score2, member2, ...]
     * @return 新添加的成员数量
     * @throws Exception 如果 key 存在但不是有序集合，或参数格式错误
     */
    public synchronized int zadd(String key, List<Object> scoresAndMembers) throws Exception {
        Object value = map.get(key);
        RedisSortedSet sortedSet;

        if (value == null) {
            sortedSet = new RedisSortedSet();
            map.put(key, sortedSet);
        } else if (value instanceof RedisSortedSet) {
            sortedSet = (RedisSortedSet) value;
        } else {
            // 如果 key 存在但不是我们期望的类型，抛出异常
            throw new WrongTypeException("Operation against a key holding the wrong kind of value");
        }

        if (scoresAndMembers.size() % 2 != 0) {
            throw new Exception("ZADD command requires score-member pairs.");
        }

        int newElements = 0;
        for (int i = 0; i < scoresAndMembers.size(); i += 2) {
            // 从列表中解析出 score 和 member
            double score = Double.parseDouble(new String((byte[]) scoresAndMembers.get(i)));
            byte[] member = (byte[]) scoresAndMembers.get(i + 1);

            // 调用 RedisSortedSet 的 add 方法
            newElements += sortedSet.add(score, member);
        }

        return newElements;
    }
    /**
     * ===> 新增方法 <===
     * 获取有序集合中指定成员的排名。
     * @param key 有序集合的 key
     * @param member 要查询的成员
     * @return 成员的排名 (long 类型)。如果 key 或 member 不存在，返回 null。
     * @throws WrongTypeException 如果 key 存在但不是有序集合。
     */
    public synchronized Long zrank(String key, byte[] member) throws WrongTypeException {
        Object value = map.get(key);

        if (value == null) {
            return null; // Key 不存在
        }

        if (!(value instanceof RedisSortedSet)) {
            throw new WrongTypeException("Operation against a key holding the wrong kind of value");
        }

        RedisSortedSet sortedSet = (RedisSortedSet) value;
        int rank = sortedSet.getRank(member);

        if (rank == -1) {
            return null; // Member 不存在
        }

        return (long) rank;
    }
}
