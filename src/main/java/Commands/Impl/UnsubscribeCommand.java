package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Service.RespEncoder;
import Storage.DataStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class UnsubscribeCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        DataStore dataStore = DataStore.getInstance();

        // 如果没有提供频道名，则从所有频道退订
        if (args.isEmpty()) {
            // 获取退订前该客户端订阅的所有频道
            Set<String> channelsBefore = dataStore.getClientSubscriptions().get(context.getOutputStream());
            if (channelsBefore != null && !channelsBefore.isEmpty()) {
                // 创建一个副本进行遍历
                List<String> channelsToUnsubscribe = new ArrayList<>(channelsBefore);
                for (String channel : channelsToUnsubscribe) {
                    dataStore.unsubscribe(channel, context.getOutputStream());
                    sendResponse(channel, context);
                }
            }
        } else {
            // 遍历所有请求退订的频道
            for (byte[] channelBytes : args) {
                String channel = new String(channelBytes, StandardCharsets.UTF_8);
                dataStore.unsubscribe(channel, context.getOutputStream());
                sendResponse(channel, context);
            }
        }

        // UNSUBSCRIBE 命令本身不返回数据给命令处理循环，因为它直接写入流
        return Command.STATE_CHANGE_SUBSCRIBE;
    }
    /**
     * 辅助方法，用于向客户端发送标准的退订响应
     */
    private void sendResponse(String channel, CommandContext context) {
        try {
            DataStore dataStore = DataStore.getInstance();
            int remainingSubscriptions = dataStore.getSubscriptionCountForClient(context.getOutputStream());

            List<Object> responsePayload = new ArrayList<>();
            responsePayload.add("unsubscribe".getBytes(StandardCharsets.UTF_8));
            responsePayload.add(channel.getBytes(StandardCharsets.UTF_8));
            responsePayload.add(remainingSubscriptions);

            RespEncoder.encode(context.getOutputStream(), responsePayload);
            context.getOutputStream().flush();
        } catch (IOException e) {
            // 在实际项目中，这里应该有更完善的日志和异常处理
            e.printStackTrace();
        }
    }
}
