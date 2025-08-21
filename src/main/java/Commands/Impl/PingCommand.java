package Commands.Impl;

import Commands.Command;

import java.io.OutputStream;
import java.util.List;

/**
 * @author Achilles
 * 处理PING命令
 * @create 2020-04-07 下午 04:05
 * PING -> 返回 PONG (Simple String)
 * PING <message> -> 返回 <message> (Bulk String)
 */
public class PingCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, OutputStream os) {
        if(args.isEmpty()){
            //情况1：命令是PING 没有参数，返回PONG
            //ClientHandler的sendResponse方法会把他编码成“+PONG\r\n”
            return "PONG";
        } else if (args.size()==1) {
            //情况2：命令是PING <message> 返回参数本身（一个字节数组）
            // ClientHandler 的 sendResponse 方法会把它编码成 Bulk String
            return args.get(0);
        }else {
            //情况3:参数数量错误，返回一个异常
            // 返回一个异常，ClientHandler 会把它编码成错误信息
            return new Exception("wrong number of arguments for 'ping' command");
        }
    }
}
