package Commands.Impl;

import Commands.Command;
import Storage.DataStore;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Achilles
 * 实现了 SUBSCRIBE 命令。
 */
public class SubscribeCommand implements Command {

    /**
     * 一个简单的内部类，用作 SUBSCRIBE 命令成功时的特定返回类型。
     * ClientHandler 将使用 'instanceof' 来识别它。
     */
    public static class SubscribeResult {
        public final List<Object> responsePayload;
        public SubscribeResult(List<Object> payload) {
            this.responsePayload = payload;
        }
    }

    @Override
    public Object execute(List<byte[]> args, OutputStream os) {
        if (args.isEmpty()) {
            return new Exception("wrong number of arguments for 'subscribe' command");
        }

        DataStore dataStore = DataStore.getInstance();
        int successfulSubscriptions = 0;
        List<Object> responsePayload = new ArrayList<>();

        // 遍历所有请求订阅的频道
        for (byte[] channelBytes : args) {
            String channel = new String(channelBytes, StandardCharsets.UTF_8);

            dataStore.subscribe(channel, os);
            successfulSubscriptions++;

            // 构建单次订阅的确认消息
            // 对于这个阶段，我们只处理一个频道
            responsePayload.add("subscribe".getBytes(StandardCharsets.UTF_8));
            responsePayload.add(channelBytes);
            responsePayload.add(successfulSubscriptions);

            // 将响应数据包装在我们的特定结果类型中并返回
            return new SubscribeResult(responsePayload);
        }

        // 理论上不应该执行到这里
        return new Exception("Failed to subscribe to any channel");
    }
}
