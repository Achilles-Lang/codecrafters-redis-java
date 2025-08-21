package Commands.Impl;

import Commands.Command;
import Service.FullResyncResponse;
import Storage.DataStore;
import Storage.ReplicationInfo;

import java.io.OutputStream;
import java.util.List;

/**
 * @author Achilles
 */
public class PsyncCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, OutputStream os) {
        DataStore.getInstance().addReplica(os);

        ReplicationInfo info = DataStore.getInstance().getReplicationInfo();
        return new FullResyncResponse(info.getMasterReplid(), info.getMasterReplOffset());
    }
}
