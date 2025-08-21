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
    private final CommandHandler commandHandler;

    public MasterConnectionHandler(String host, int port, int listeningPort, CommandHandler commandHandler) {
        this.masterHost = host;
        this.masterPort = port;
        this.listeningPort = listeningPort;
        this.commandHandler = commandHandler;
    }

    @Override
    public void run() {
        try (Socket masterSocket = new Socket(masterHost, masterPort)) {
            OutputStream os = masterSocket.getOutputStream();
            InputStream is = masterSocket.getInputStream();

            // **关键修复**: 创建一个 Parser 实例，并用它来处理所有读取操作
            Protocol parser = new Protocol(is);

            // --- 阶段 1: PING ---
            sendCommand(os, "PING");
            parser.readSimpleString(); // 使用 Parser 读取 PONG

            // --- 阶段 2: REPLCONF ---
            sendCommand(os, "REPLCONF", "listening-port", String.valueOf(this.listeningPort));
            parser.readSimpleString(); // 使用 Parser 读取 OK
            sendCommand(os, "REPLCONF", "capa", "psync2");
            parser.readSimpleString(); // 使用 Parser 读取 OK

            // --- 阶段 3: PSYNC ---
            sendCommand(os, "PSYNC", "?", "-1");
            parser.readSimpleString(); // 使用 Parser 读取 +FULLRESYNC...

            // 读取 RDB 文件
            parser.readRdbFile();
            System.out.println("Handshake successful. Listening for propagated commands.");

            // --- 命令处理循环 ---
            while (!masterSocket.isClosed()) {
                List<byte[]> commandParts = parser.readCommand();
                if (commandParts == null) {
                    break;
                }

                String commandName = new String(commandParts.get(0), StandardCharsets.UTF_8);
                List<byte[]> args = commandParts.subList(1, commandParts.size());
                Command command = this.commandHandler.getCommand(commandName);

                if (command != null) {
                    command.execute(args, null); // 在从节点执行，无需上下文
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

}
