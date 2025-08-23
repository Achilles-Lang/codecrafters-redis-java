package Commands.Impl;

import Commands.Command;
import Service.RespEncoder;
import Storage.DataStore;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BlpopCommand implements Command {

    /**
     * 一个特殊的静态对象，用作信号。
     * 当命令返回这个对象时，表示它已经自己处理了响应发送。
     */
    public static final Object RESPONSE_ALREADY_SENT = new Object();

    @Override
    public Object execute(List<byte[]> args, OutputStream os) {
        if (args.size() < 2) {
            return new Exception("wrong number of arguments for 'blpop' command");
        }

        try {
            double timeout = Double.parseDouble(new String(args.get(args.size() - 1), StandardCharsets.UTF_8));
            List<byte[]> keys = args.subList(0, args.size() - 1);

            Object[] result = DataStore.getInstance().blpop(keys, timeout);

            if (result == null) {
                // 超时，发送 RESP Null
                RespEncoder.encode(os, null);
            } else {
                // 成功获取，构建并发送响应数组
                if (result.length == 2 && result[0] instanceof byte[] && result[1] instanceof byte[]) {
                    List<byte[]> responseList = new ArrayList<>();
                    responseList.add((byte[]) result[0]); // key
                    responseList.add((byte[]) result[1]); // value
                    RespEncoder.encode(os, responseList);
                } else {
                    RespEncoder.encode(os, new Exception("Internal error: DataStore returned unexpected format for BLPOP"));
                }
            }
            // 返回信号，告诉 ClientHandler 无需再做任何事
            return RESPONSE_ALREADY_SENT;

        } catch (Exception e) {
            // 捕获所有异常（包括 NumberFormatException 和 InterruptedException）
            try {
                RespEncoder.encode(os, e);
            } catch (IOException ioException) {
                // 如果连错误信息都发不出去，就在服务器端打印日志
                ioException.printStackTrace();
            }
            return RESPONSE_ALREADY_SENT;
        }
    }
}
