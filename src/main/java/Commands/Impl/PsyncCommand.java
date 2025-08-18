package Commands.Impl;

import Commands.Command;
import Storage.DataStore;
import Storage.ReplicationInfo;

import java.util.List;

/**
 * @author Achilles
 */
public class PsyncCommand implements Command {
    @Override
    public Object execute(List<byte[]> args) {
        ReplicationInfo info= DataStore.getInstance().getReplicationInfo();
        String replid=info.getMasterReplid();
        long offset=info.getMasterReplOffset();

        String response="FULLRESYNC " + replid + " " + offset;
        return response;
    }
}
