package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Storage.DataStore;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WaitCommand implements Command {

    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if (args.size() != 2) {
            return new Exception("wrong number of arguments for 'wait' command");
        }

        try {
            int numReplicasToWaitFor = Integer.parseInt(new String(args.get(0), StandardCharsets.UTF_8));
            long timeout = Long.parseLong(new String(args.get(1), StandardCharsets.UTF_8));

            DataStore dataStore = DataStore.getInstance();
            int connectedReplicas = dataStore.getReplicaCount();

            // 如果没有副本或不要求等待，立即返回0
            if (connectedReplicas == 0 || numReplicasToWaitFor == 0) {
                return 0L;
            }

            // 获取 Master 当前的写入偏移量，这是我们需要达到的目标
            long targetOffset = dataStore.getMasterOffset();

            // 如果没有任何写操作，说明所有副本都是同步的
            if (targetOffset == 0) {
                return (long) connectedReplicas;
            }

            // 使用 AtomicInteger 来安全地在多线程中计数
            AtomicInteger syncedReplicasCount = new AtomicInteger(0);

            // 使用 CountDownLatch 来等待所有副本的响应
            CountDownLatch latch = new CountDownLatch(connectedReplicas);

            // 设置一个回调，当 Master 收到 ACK 时会执行这个回调
            DataStore.AckCallback callback = (replicaOffset) -> {
                if (replicaOffset >= targetOffset) {
                    syncedReplicasCount.incrementAndGet();
                }
                latch.countDown(); // 无论是否同步，都减少 latch 计数
            };

            // 将回调注册到 DataStore
            dataStore.registerAckCallback(callback);

            // 向所有副本广播 GETACK 命令
            dataStore.broadcastToReplicas("REPLCONF", "GETACK", "*");

            try {
                // 等待所有副本响应，或者超时
                latch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                // 确保回调被移除，防止内存泄漏
                dataStore.removeAckCallback(callback);
            }

            return (long) syncedReplicasCount.get();

        } catch (NumberFormatException e) {
            return new Exception("value is not an integer or out of range");
        }
    }
}
