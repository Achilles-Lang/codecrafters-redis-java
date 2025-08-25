package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Service.RespEncoder;
import Storage.DataStore;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author Achilles
 * @created 05/11/2023 - 1:05 pm
 * @project Redis
 *
 */
public class PublishCommand implements Command {

    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if (args.size() < 2) {
            // 参数数量不足，返回错误
            return new Exception("wrong number of arguments for 'publish' command");
        }

        String channelName = new String(args.get(0), StandardCharsets.UTF_8);
        // String message = new String(args.get(1), StandardCharsets.UTF_8); // 消息内容本阶段用不到

        // 1. 获取 DataStore 单例
        DataStore dataStore = DataStore.getInstance();

        // 2. 调用我们刚刚新增的方法获取订阅者数量
        int subscriberCount = dataStore.getSubscriberCount(channelName);

        // 3. 使用 RespEncoder 将整数结果直接发送给客户端
        try {
            OutputStream os = context.getOutputStream();
            RespEncoder.encode(os, subscriberCount);
            os.flush();
        } catch (IOException e) {
            // 如果发送失败，可以记录日志或返回一个错误
            System.out.println("Error writing PUBLISH response: " + e.getMessage());
        }

        // PUBLISH 命令本身不改变客户端的订阅状态，也不需要特殊的返回值
        // 返回 null 表示正常执行完毕
        return null;
    }
}
