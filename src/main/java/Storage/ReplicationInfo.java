package Storage;

/**
 * @author Achilles
 */
public class ReplicationInfo {
    private String role = "master";
    private String masterHost;
    private int masterPort;
    private String masterReplid = "0";
    private int masterReplOffset = 0;

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

    public void setMasterReplid(String masterReplid) {
        this.masterReplid = masterReplid;
    }

    public int getMasterReplOffset() {
        return masterReplOffset;
    }

    public void setMasterReplOffset(int masterReplOffset) {
        this.masterReplOffset = masterReplOffset;
    }
}
