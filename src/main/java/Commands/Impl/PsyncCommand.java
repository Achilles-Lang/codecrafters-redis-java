package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
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
    public Object execute(List<byte[]> args, CommandContext context) {
        DataStore.getInstance().addReplica(context.getOutputStream());

        ReplicationInfo info = DataStore.getInstance().getReplicationInfo();
        return new FullResyncResponse(info.getMasterReplid(), info.getMasterReplOffset());
    }
}
