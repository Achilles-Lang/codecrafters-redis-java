package Commands.Impl;

import Commands.Command;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Achilles
 * 处理PING命令
 * @create 2020-04-07 下午 04:05
 * PING -> 返回 PONG (Simple String)
 * PING <message> -> 返回 <message> (Bulk String)
 */
public class PingCommand implements Command {
    //存储客户端的状态
    private boolean isClientSubscribed=false;

    public void setClientSubscribed(boolean isClientSubscribed){
        this.isClientSubscribed=isClientSubscribed;
    }

    @Override
    public Object execute(List<byte[]> args, OutputStream os) {
        if(this.isClientSubscribed){
            //订阅模式下
            List<byte[]> response=new ArrayList<>();
            response.add("pong".getBytes(StandardCharsets.UTF_8));

            if(args.isEmpty()){
                //如果PING带有参数，则返回该参数，否则返回空字符串
                response.add("".getBytes(StandardCharsets.UTF_8));
            } else {
                response.add(args.get(0));
            }
            return response;
        } else {
            //非订阅模式下
            if(args.isEmpty()){
                return "PONG".getBytes(StandardCharsets.UTF_8);
            } else {
                return args.get(0);
            }
        }
    }
}
