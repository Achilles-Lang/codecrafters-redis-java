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
            Protocol rdbParser = new Protocol(inputStream);
            rdbParser.readRdbFile();
            // 读取主节点对 PSYNC 的响应
            // 响应会是 "+FULLRESYNC <master_replid> <offset>\r\n"
            String psyncResponse = readResponse(inputStream);
            System.out.println("Received PSYNC response: " + psyncResponse);

            Protocol commandParser = new Protocol(inputStream);
            while (!masterSocket.isClosed()) {
                // 1. 从主节点连接中读取并解析命令
                List<byte[]> commandParts = commandParser.readCommand();
                if (commandParts == null) {
                    break; // 连接已关闭
                }

                System.out.println("Received propagated command from master.");

                // 2. 查找并执行命令
                String commandName = new String(commandParts.get(0), StandardCharsets.UTF_8);
                List<byte[]> args = commandParts.subList(1, commandParts.size());
                Command command = this.commandHandler.getCommand(commandName);

                if (command != null) {
                    // 3. 执行命令，但忽略返回值，不发送任何响应
                    // 注意：因为从节点不需要响应或注册自己，所以 clientHandler 参数可以传 null
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
