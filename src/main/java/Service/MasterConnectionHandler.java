package Service;

import Commands.Command;
import Commands.CommandHandler;

import java.io.BufferedInputStream;
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
public class MasterConnectionHandler implements Runnable {
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
            // 使用 BufferedInputStream 提高读取效率
            InputStream is = new BufferedInputStream(masterSocket.getInputStream());

            // 假设你有一个 Protocol 类来处理 RESP 解析
            // 如果没有，你需要实现相应的解析逻辑
            Protocol parser = new Protocol(is);

            // --- 阶段 1: PING ---
            System.out.println("Sending PING to master...");
            sendCommand(os, "PING");
            String pingResponse = parser.readSimpleString();
            System.out.println("Received from master: " + pingResponse);


            // --- 阶段 2: REPLCONF ---
            System.out.println("Sending REPLCONF listening-port...");
            sendCommand(os, "REPLCONF", "listening-port", String.valueOf(this.listeningPort));
            String replconfPortResponse = parser.readSimpleString();
            System.out.println("Received from master: " + replconfPortResponse);

            System.out.println("Sending REPLCONF capa psync2...");
            sendCommand(os, "REPLCONF", "capa", "psync2");
            String replconfCapaResponse = parser.readSimpleString();
            System.out.println("Received from master: " + replconfCapaResponse);


            // --- 阶段 3: PSYNC ---
            System.out.println("Sending PSYNC...");
            sendCommand(os, "PSYNC", "?", "-1");
            String psyncResponse = parser.readSimpleString();
            System.out.println("Received from master: " + psyncResponse);


            // --- **关键修复**: 处理 RDB 文件 ---
            // +FULLRESYNC 响应之后，Master 会立即发送 RDB 文件
            // RDB 文件以 RESP Bulk String 的格式发送: $<length>\r\n<binary-data>
            System.out.println("Waiting for RDB file...");
            // 读取第一个字节，应该是 '$'
            int firstByte = is.read();
            if (firstByte != '$') {
                throw new IOException("Expected '$' for RDB file bulk string, but got: " + (char)firstByte);
            }

            // 读取 RDB 文件的长度
            StringBuilder lengthBuilder = new StringBuilder();
            int nextByte;
            while ((nextByte = is.read()) != '\r') {
                lengthBuilder.append((char) nextByte);
            }
            // 跳过 '\n'
            is.read();

            int rdbLength = Integer.parseInt(lengthBuilder.toString());
            System.out.println("RDB file length: " + rdbLength);

            // 读取并丢弃 RDB 文件的二进制内容
            if (rdbLength > 0) {
                // 使用 readNBytes 确保读取了所有字节
                byte[] rdbContent = new byte[rdbLength];
                is.read(rdbContent, 0, rdbLength);
                System.out.println("RDB file received and processed.");
            }

            System.out.println("Handshake successful. Listening for propagated commands.");

            // --- 命令处理循环 ---
            while (!masterSocket.isClosed()) {
                // 现在输入流中只剩下 Master 传播过来的命令
                List<byte[]> commandParts = parser.readCommand();
                if (commandParts == null) {
                    // 连接关闭
                    break;
                }

                String commandName = new String(commandParts.get(0), StandardCharsets.UTF_8).toUpperCase();
                System.out.println("Received propagated command: " + commandName);

                List<byte[]> args = commandParts.subList(1, commandParts.size());
                Command command = this.commandHandler.getCommand(commandName);

                if (command != null) {
                    // 在从节点执行命令，但不需要发送响应回 Master
                    // 所以第二个参数传入 null
                    command.execute(args, null);
                } else {
                    System.out.println("Unknown propagated command: " + commandName);
                }
            }

        } catch (IOException e) {
            System.out.println("IOException in MasterConnectionHandler: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // 辅助方法，用于将命令编码为RESP Array 格式并发送
    private void sendCommand(OutputStream os, String... args) throws IOException {
        StringBuilder commandBuilder = new StringBuilder();
        commandBuilder.append("*").append(args.length).append("\r\n");
        for (String arg : args) {
            commandBuilder.append("$").append(arg.getBytes(StandardCharsets.UTF_8).length).append("\r\n");
            commandBuilder.append(arg).append("\r\n");
        }
        os.write(commandBuilder.toString().getBytes(StandardCharsets.UTF_8));
        os.flush();
    }
}
