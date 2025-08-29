package Storage;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author Achilles
 * Redis 有序集合的内部实现
 */
public class RedisSortedSet {
    // 使用 ConcurrentSkipListMap 来同时满足排序和快速查找
    // Key: SortedSetEntry (包含 score 和 member)
    // Value: member (byte[])
    // 这样设计是为了利用 SkipList 的排序特性，同时又能通过 member 快速查找

    private final NavigableMap<SortedSetEntry, byte[]> sortedEntries = new ConcurrentSkipListMap<>();
    private final Map<String,Double> memberScores = new HashMap<>();

    /**
     * 添加或更新一个成员。
     * @return 如果是新添加的成员，返回 1，否则返回 0。
     */
    public synchronized int add(double score, byte[] member) {
        String memberStr = new String(member);

        // 检查成员是否已存在
        if (memberScores.containsKey(memberStr)) {
            double oldScore = memberScores.get(memberStr);
            // 如果分数没变，什么都不做
            if (oldScore == score) {
                return 0;
            }
            // 如果分数变了，先移除旧条目
            sortedEntries.remove(new SortedSetEntry(oldScore, member));
        }

        // 添加新条目或更新后的条目
        sortedEntries.put(new SortedSetEntry(score, member), member);
        memberScores.put(memberStr, score);

        // 如果是新成员，返回 1
        return memberScores.containsKey(memberStr) ? 0 : 1;
    }
}
