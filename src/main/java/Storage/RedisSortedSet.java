// 文件路径: src/main/java/Storage/RedisSortedSet.java

package Storage;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

/**
 * @author Achilles
 * 有序集合 (Sorted Set) 的实现类。
 * 结合了 ConcurrentSkipListMap (按分数排序) 和 HashMap (快速访问成员)，
 * 以实现高效的 ZADD, ZRANGE 等操作。
 */
public class RedisSortedSet {

    // 用于按分数排序存储条目。Key 是一个包含分数和成员的对象，Value 是成员的 byte[] 形式。
    // ConcurrentSkipListMap 是一个线程安全的、有序的 Map，非常适合实现有序集合。
    private final ConcurrentSkipListMap<SortedSetEntry, byte[]> sortedEntries = new ConcurrentSkipListMap<>();

    // 用于快速检查成员是否存在，并获取其分数。Key 是成员的 String 形式，Value 是分数。
    private final Map<String, Double> memberScores = new HashMap<>();

    /**
     * 向有序集合中添加或更新一个成员。
     *
     * @param score  成员的分数
     * @param member 成员的字节数组
     * @return 如果是新添加的成员，返回 1；如果是更新现有成员的分数，返回 0。
     */
    public synchronized int add(double score, byte[] member) {
        String memberStr = new String(member);

        // 使用一个局部变量来记录这是否是一个新成员，以避免逻辑错误。
        boolean isNewMember = !memberScores.containsKey(memberStr);

        // 如果成员已存在，需要先从 sortedEntries 中移除旧的条目
        if (!isNewMember) {
            double oldScore = memberScores.get(memberStr);
            // 为了比较，我们需要创建一个临时的旧条目对象
            // 注意：这里的 member 是旧的 member byte[]，但因为 String(member) 相同，所以 hashCode 和 equals 也能匹配
            sortedEntries.remove(new SortedSetEntry(oldScore, member));
        }

        // 添加/更新分数映射
        memberScores.put(memberStr, score);
        // 添加/更新排序映射
        sortedEntries.put(new SortedSetEntry(score, member), member);

        // 根据我们最初的判断返回结果
        return isNewMember ? 1 : 0;
    }

    /**
     * 一个内部类，代表有序集合中的一个条目。
     * 它实现了 Comparable 接口，以便 ConcurrentSkipListMap 可以根据它进行排序。
     * 主要排序依据是 score，次要排序依据是 member (按字典序)。
     */
    private static class SortedSetEntry implements Comparable<SortedSetEntry> {
        final double score;
        final byte[] member;

        SortedSetEntry(double score, byte[] member) {
            this.score = score;
            this.member = member;
        }
        public byte[] getMember() {
            return member;
        }

        @Override
        public int compareTo(SortedSetEntry other) {
            // 首先比较分数
            int scoreCompare = Double.compare(this.score, other.score);
            if (scoreCompare != 0) {
                return scoreCompare;
            }
            // 如果分数相同，则比较成员的字典序
            return new String(this.member).compareTo(new String(other.member));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SortedSetEntry that = (SortedSetEntry) o;
            // 只要成员相同，就认为是同一个条目 (用于 remove 操作)
            return new String(this.member).equals(new String(that.member));
        }

        @Override
        public int hashCode() {
            // 只要成员相同，就认为是同一个条目 (用于 remove 操作)
            return Objects.hash(new String(this.member));
        }

    }

    /**
     * ===> 新增方法 <===
     * 获取指定成员的排名 (从 0 开始的索引)。
     *
     * @param member 要查询的成员
     * @return 成员的排名。如果成员不存在，返回 -1。
     */
    public synchronized int getRank(byte[] member) {
        String memberStr = new String(member);
        // 1. 检查成员是否存在
        if (!memberScores.containsKey(memberStr)) {
            return -1; // -1 表示成员不存在
        }

        // 2. 获取成员的分数
        double score = memberScores.get(memberStr);

        // 3. 构造用于查找的目标条目
        SortedSetEntry targetEntry = new SortedSetEntry(score, member);

        // 4. 遍历有序的 Map 来找到索引
        // 这是一个简单但有效的 O(n) 实现
        int rank = 0;
        for (SortedSetEntry entry : sortedEntries.keySet()) {
            if (entry.equals(targetEntry)) {
                return rank;
            }
            rank++;
        }

        return -1; // 理论上不应该到达这里，但作为保护
    }

    /**
     * ===> 新增方法 <===
     * <p>
     * 获取指定排名范围内的所有成员。
     *
     * @param start 起始排名 (包含)
     * @param stop  结束排名 (包含)
     * @return 包含指定范围内所有成员的列表。
     */
    public synchronized List<byte[]> getRange(int start, int stop) {
        List<SortedSetEntry> entryList = new ArrayList<>(sortedEntries.keySet());
        int size = entryList.size();

        if (start < 0) {
            start = size+start;
        } // 简单保护
        if (stop >= size) {
            stop = size + stop;
        }
        if(start<0){
            start=0;
        }
        if(stop>=size){
            stop=size-1;
        }

// 3. 如果起始索引无效或范围无效，返回空列表
        if (start >= size || start > stop) {
            return new ArrayList<>();
        }

// 4. 截取子列表并提取成员
        return entryList.subList(start, stop + 1) // subList 的第二个参数是 exclusive，所以要 +1
                .stream()
                .map(SortedSetEntry::getMember)
                .collect(Collectors.toList());
    }
}


