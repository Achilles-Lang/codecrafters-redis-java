package Commands;

import java.io.OutputStream;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author Achilles
 * 一个上下文对象，封装了执行命令时可能需要的所有客户端特定信息。
 * 这使得命令类本身可以保持无状态。
 */
public class CommandContext {
    private final OutputStream outputStream;
    private  boolean isClientSubscribed=false;
    private long replicaOffset=0;

    public CommandContext(OutputStream os,boolean isClientSubscribed){
        this.outputStream= os;
        this.isClientSubscribed=isClientSubscribed;
    }

    public CommandContext(OutputStream os,long replicaOffset) {
        this.outputStream= os;
        this.replicaOffset=replicaOffset;
    }

    public OutputStream getOutputStream(){
        return outputStream;
    }

    public boolean isClientSubscribed(){
        return isClientSubscribed;
    }
    public long getReplicaOffset(){
        return replicaOffset;
    }
    public void enterSubscribeMode(){
        this.isClientSubscribed=true;
    }
    public void exitSubscribeMode(){
        this.isClientSubscribed=false;
    }
}
