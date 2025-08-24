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
    private final boolean isClientSubscribed;

    public CommandContext(OutputStream os,boolean isClientSubscribed){
        this.outputStream= os;
        this.isClientSubscribed=isClientSubscribed;
    }

    public OutputStream getOutputStream(){
        return outputStream;
    }

    public boolean isClientSubscribed(){
        return isClientSubscribed;
    }
}
