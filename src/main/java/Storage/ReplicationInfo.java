package Storage;

/**
 * @author Achilles
 */
public class ReplicationInfo {
    private String role = "master";
    private String masterHost;
    private int masterPort;
    private final String masterReplid = "8371b4fb1155b71f4a04d3e1bc3e18c469de59a0";
    private final long masterReplOffset = 0L;

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public void setMasterHost(String masterHost) {
        this.masterHost = masterHost;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public void setMasterPort(int masterPort) {
        this.masterPort = masterPort;
    }

    public String getMasterReplid() {
        return masterReplid;
    }

    public long getMasterReplOffset() {
        return masterReplOffset;
    }


}
