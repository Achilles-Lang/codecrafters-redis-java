package Storage;

/**
 * @author Achilles
 * 数据模型：字符串
 */
public class ValueEntry {
    public final byte[] value;

    // **关键修复**: 将类型从原始 long 修改为包装 Long，使其可以接受 null
    final Long expiryTimestamp; // 过期的绝对时间点 (毫秒), null 表示永不过期

    // **关键修复**: 构造函数的参数类型也修改为 Long
    public ValueEntry(byte[] value, Long expiryTimestamp) {
        this.value = value;
        this.expiryTimestamp = expiryTimestamp;
    }

    /**
     * 检查这个条目是否已经过期。
     * @return 如果已过期则返回 true，否则返回 false。
     */
    public boolean isExpired() {
        // **关键修复**: 检查 expiryTimestamp 是否为 null
        if (expiryTimestamp == null) {
            return false; // 永不过期
        }
        return System.currentTimeMillis() > expiryTimestamp;
    }
}
