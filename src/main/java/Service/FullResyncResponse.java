package Service;

/**
 * @author Achilles
 */
public class FullResyncResponse {
    private final String masterReplid;
    private final long masterReplOffset;

    public FullResyncResponse(String masterReplid, long masterOffset) {
        this.masterReplid = masterReplid;
        this.masterReplOffset = masterOffset;
    }

    public String getMasterReplid() {
        return masterReplid;
    }

    public long getMasterReplOffset() {
        return masterReplOffset;
    }
}
