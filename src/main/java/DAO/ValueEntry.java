package DAO;

/**
 * @author Achilles
 * 用于封装存储在DataStore中的值及其元数据
 */
public class ValueEntry {
    public final byte[] value;
    final long expiryTimestamp; // 过期的绝对时间点 (毫秒)

    // expiryTimestamp = -1 表示永不过期
    public ValueEntry(byte[] value, long expiryTimestamp) {
        this.value = value;
        this.expiryTimestamp = expiryTimestamp;
    }

    /**
     * 检查这个条目是否已经过期。
     * @return 如果已过期则返回 true，否则返回 false。
     */
    public boolean isExpired() {
        if (expiryTimestamp == -1) {
            return false; // 永不过期
        }
        return System.currentTimeMillis() > expiryTimestamp;
    }
}
