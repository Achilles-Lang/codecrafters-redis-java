import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * @author Achilles
 */
public class DataStore {
    private static final Map<String, byte[]> map = new ConcurrentHashMap<>();

    public static void set(String key, byte[] value) {
        map.put(key, value);
    }

    public static byte[] get(String key) {
        return map.get(key);
    }
}
