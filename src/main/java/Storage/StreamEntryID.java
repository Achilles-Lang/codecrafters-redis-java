package Storage;

/**
 * @author Achilles
 * 用于实现StreamID
 */
public class StreamEntryID implements Comparable<StreamEntryID>{

    public final long timestamp;
    public final int sequence;

    public StreamEntryID(long timestamp, int sequence) {
        this.timestamp = timestamp;
        this.sequence = sequence;
    }

    @Override
    public String toString() {
        return timestamp + "-" + sequence;
    }

    @Override
    public int compareTo(StreamEntryID other) {
        if(this.timestamp!=other.timestamp){
            return Long.compare(this.timestamp, other.timestamp);
        }
        return Integer.compare(this.sequence, other.sequence);
    }
}
