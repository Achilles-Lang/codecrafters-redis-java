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
    private final int listeningPort;

    public MasterConnectionHandler(String masterHost, int masterPort,int listeningPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.listeningPort=listeningPort;
    }

    @Override
    public void run() {
        try {
            Socket masterSocket = new Socket(masterHost, masterPort);
            OutputStream outputStream = masterSocket.getOutputStream();
            InputStream inputStream = masterSocket.getInputStream();

            //阶段1：PING
            sendCommand(outputStream, "PING");
            readResponse(inputStream);

            //2
            sendCommand(outputStream, "REPLCONF", "listening-port", String.valueOf(this.listeningPort));
            readResponse(inputStream); // 假设已验证是 OK
            sendCommand(outputStream, "REPLCONF", "capa", "psync2");
            readResponse(inputStream); // 假设已验证是 OK

            // --- **新增**: 阶段 3: 发送 PSYNC ---
            System.out.println("Sending PSYNC ? -1");
            sendCommand(outputStream, "PSYNC", "?", "-1");

            // 读取主节点对 PSYNC 的响应
            // 响应会是 "+FULLRESYNC <master_replid> <offset>\r\n"
            String psyncResponse = readResponse(inputStream);
            System.out.println("Received PSYNC response: " + psyncResponse);


        } catch (IOException e){
            System.out.println("IOException in MasterConnectionHandler: " + e.getMessage());
        }
    }
    //辅助方法，用于将命令编码为RESP Array 格式并发送
    private void sendCommand(OutputStream os, String... args) throws IOException {
        StringBuilder commandBuilder = new StringBuilder();
        commandBuilder.append("*").append(args.length).append("\r\n");
        for (String arg : args) {
            commandBuilder.append("$").append(arg.length()).append("\r\n");
            commandBuilder.append(arg).append("\r\n");
        }
        os.write(commandBuilder.toString().getBytes(StandardCharsets.UTF_8));
        os.flush();
    }
    //辅助方法，用于读取响应
    private  String readResponse(InputStream is) throws IOException {
        byte[] buffer = new byte[1024];
        int bytesRead = is.read(buffer);
        if(bytesRead == -1){
            throw new IOException("Stream closed by master");
        }
        return new String(buffer, 0, bytesRead,StandardCharsets.UTF_8);
    }
}
