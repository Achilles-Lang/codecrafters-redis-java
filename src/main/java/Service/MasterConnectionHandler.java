package Service;

import Commands.Command;
import Commands.CommandHandler;

import javax.swing.text.html.parser.Parser;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author Achilles
 * 作为一个客户端，处理与主节点的连接和通信
 */
public class MasterConnectionHandler implements Runnable{
    private final String masterHost;
    private final int masterPort;
    private final int listeningPort;
    private final CommandHandler commandHandler ;

    public MasterConnectionHandler(String masterHost, int masterPort, int listeningPort, CommandHandler commandHandler) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.listeningPort=listeningPort;
        this.commandHandler = commandHandler;
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
            readResponse(inputStream);
            sendCommand(outputStream, "REPLCONF", "capa", "psync2");
            readResponse(inputStream);

            // --- **新增**: 阶段 3: 发送 PSYNC ---
            System.out.println("Sending PSYNC ? -1");
            sendCommand(outputStream, "PSYNC", "?", "-1");
            readResponse(inputStream);
            // 读取 RDB 文件
            Protocol rdbParser = new Protocol(inputStream);
            rdbParser.readRdbFile();

            System.out.println("Handshake successful. Listening for propagated commands.");

            // --- **新增**: 命令处理循环 ---
            Protocol commandParser = new Protocol(inputStream);
            while (!masterSocket.isClosed()) {
                // 1. 从主节点连接中读取并解析命令
                List<byte[]> commandParts = commandParser.readCommand();
                if (commandParts == null) {
                    break; // 连接已关闭
                }

                // 2. 查找并执行命令
                String commandName = new String(commandParts.get(0), StandardCharsets.UTF_8);
                List<byte[]> args = commandParts.subList(1, commandParts.size());
                Command command = this.commandHandler.getCommand(commandName);

                if (command != null) {
                    // 3. 执行命令以更新从节点自己的 DataStore
                    // 我们不需要响应，所以 ClientHandler 上下文传 null
                    command.execute(args, null);
                }
            }

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
