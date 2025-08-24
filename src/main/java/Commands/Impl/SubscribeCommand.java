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
 * 实现了 SUBSCRIBE 命令
 */
public class SubscribeCommand implements Command {
    /**
     * 一个特殊的静态对象，用作信号。
     * 当命令返回这个对象时，表示它已经自己处理了响应发送，并且需要改变客户端的状态。
     */
    public static final Object STATE_CHANGED_TO_SUBSCRIBED = new Object();


    @Override
    public Object execute(List<byte[]> args, OutputStream os) {
        if(args.isEmpty()){
            return new Exception("wrong number of arguments for 'subscribe' command");
        }

        DataStore dataStore = DataStore.getInstance();
        int successfulSubscriptions = 0;

        try {
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

                //发送响应
                RespEncoder.encode(os, singleSubscribeResponse);
                os.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return STATE_CHANGED_TO_SUBSCRIBED;
    }
}
