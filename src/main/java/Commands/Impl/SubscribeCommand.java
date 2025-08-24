package Commands.Impl;

import Commands.Command;
import Storage.DataStore;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Achilles
 * 实现了 SUBSCRIBE 命令
 */
public class SubscribeCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, OutputStream os) {
        if(args.isEmpty()){
            return new Exception("wrong number of arguments for 'subscribe' command");
        }

        DataStore dataStore = DataStore.getInstance();
        List<Object> response =new ArrayList<>();
        int successfulSubscriptions = 0;

        // 遍历所有请求订阅的频道
        for (byte[] channelBytes : args) {
            String channel = new String(channelBytes, StandardCharsets.UTF_8);

            //执行订阅操作
            dataStore.subscribe(channel, os);
            successfulSubscriptions++;

            //构建单次订阅的确认消息
            List<Object> singleSubscribeResponse = new ArrayList<>();
            singleSubscribeResponse.add("subscribe".getBytes(StandardCharsets.UTF_8));
            singleSubscribeResponse.add(channelBytes);
            singleSubscribeResponse.add(successfulSubscriptions);

            return singleSubscribeResponse;
        }
        return new Exception("Failed to subscribe to any channel");
    }
}
