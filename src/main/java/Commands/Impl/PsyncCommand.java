package Commands.Impl;

import Commands.Command;
import Service.ClientHandler;
import Service.FullResyncResponse;
import Storage.DataStore;
import Storage.ReplicationInfo;

import javax.imageio.IIOException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * @author Achilles
 */
public class PsyncCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, ClientHandler clientHandler) {
        try {
            DataStore.getInstance().addReplica(clientHandler.getClientSocket().getOutputStream());
        } catch (IOException e){
            e.printStackTrace();
        }
        ReplicationInfo info= DataStore.getInstance().getReplicationInfo();
        String replid=info.getMasterReplid();
        long offset=info.getMasterReplOffset();

        ReplicationInfo info1=DataStore.getInstance().getReplicationInfo();
        return new FullResyncResponse(info1.getMasterReplid(), info1.getMasterReplOffset());
    }
}
