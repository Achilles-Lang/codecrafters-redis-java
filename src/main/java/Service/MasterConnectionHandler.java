package Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * @author Achilles
 * 作为一个客户端，处理与主节点的连接和通信
 */
public class MasterConnectionHandler implements Runnable{
    private final String masterHost;
    private final int masterPort;

    public MasterConnectionHandler(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    @Override
    public void run() {
        try {
            System.out.println("Connecting to master at " + masterHost + ":" + masterPort);
            // 1. 创建一个客户端 Socket 连接到主节点
            Socket masterSocket = new Socket(masterHost, masterPort);
            System.out.println("Connected to master. Starting handshake.");

            OutputStream outputStream = masterSocket.getOutputStream();
            InputStream inputStream = masterSocket.getInputStream();

            // 2. 发送 PING 命令
            // RESP 格式: *1\r\n$4\r\nPING\r\n
            String pingCommand = "*1\r\n$4\r\nPING\r\n";
            outputStream.write(pingCommand.getBytes(StandardCharsets.UTF_8));
            outputStream.flush();
            System.out.println("Sent PING to master.");

            // 3. 读取并验证 PONG 回复
            // 简单的实现：假设缓冲区足够大，一次读完
            byte[] buffer = new byte[1024];
            int bytesRead = inputStream.read(buffer);
            String response = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);

            if ("+PONG\r\n".equals(response)) {
                System.out.println("Received PONG from master. Handshake part 1 successful.");
            } else {
                System.out.println("Error: Did not receive PONG from master. Received: " + response);
            }

            // 在后续阶段，这个线程会继续留在这里，监听主节点发来的数据
            // masterSocket.close(); // 暂时我们先不关闭连接
        } catch (IOException e){
            System.out.println("IOException in MasterConnectionHandler: " + e.getMessage());
        }
    }
}
