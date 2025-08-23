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
     * **改动 1: 新增一个特殊的静态对象，用作信号**
     * 当命令返回这个对象时，就告诉 ClientHandler：“响应我已经发了，你不用管了”。
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

            // 阻塞并获取结果
            Object[] result = DataStore.getInstance().blpop(keys, timeout);

            /**
             * **改动 2: 不再返回数据，而是直接在这里发送响应**
             * 这是整个修复的核心。
             */
            if (result == null) {
                // 超时，直接发送 RESP Null
                RespEncoder.encode(os, null);
            } else {
                // 成功获取，构建并直接发送响应数组
                if (result.length == 2 && result[0] instanceof byte[] && result[1] instanceof byte[]) {
                    List<byte[]> responseList = new ArrayList<>();
                    responseList.add((byte[]) result[0]); // key
                    responseList.add((byte[]) result[1]); // value
                    RespEncoder.encode(os, responseList);
                } else {
                    RespEncoder.encode(os, new Exception("Internal error: DataStore returned unexpected format for BLPOP"));
                }
            }

            /**
             * **改动 3: 返回我们的信号对象**
             */
            return RESPONSE_ALREADY_SENT;

        } catch (Exception e) {
            // 如果在整个过程中发生任何异常，也直接在这里发送错误响应
            try {
                RespEncoder.encode(os, e);
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
            return RESPONSE_ALREADY_SENT;
        }
    }
}
