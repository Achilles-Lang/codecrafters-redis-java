package Commands.Impl;

import Commands.Command;
import Service.RespEncoder;
import Storage.DataStore;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Achilles
 * 实现了 SUBSCRIBE 命令。
 */
public class SubscribeCommand implements Command {

    @Override
    public Object execute(List<byte[]> args, OutputStream os) {
        if (args.isEmpty()) {
            return new Exception("wrong number of arguments for 'subscribe' command");
        }

        DataStore dataStore = DataStore.getInstance();
        int successfulSubscriptions = 0;

        try {
            // 遍历所有请求订阅的频道
            for (byte[] channelBytes : args) {
                String channel = new String(channelBytes, StandardCharsets.UTF_8);
                dataStore.subscribe(channel, os);
                successfulSubscriptions++;

                // 构建单次订阅的确认消息
                // 对于这个阶段，我们只处理一个频道
                List<Object> responsePayload = new ArrayList<>();
                responsePayload.add("subscribe".getBytes(StandardCharsets.UTF_8));
                responsePayload.add(channelBytes);
                responsePayload.add(successfulSubscriptions);

                RespEncoder.encode(os, responsePayload);
                os.flush();
            }
        } catch (IOException e) {
            return e;
        }
        return Command.STATE_CHANGE_SUBSCRIBE;
    }
}
