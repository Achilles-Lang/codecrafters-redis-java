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
    private long bytesProcessed = 0;

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
            InputStream is = new BufferedInputStream(masterSocket.getInputStream());
            Protocol parser = new Protocol(is);

            // --- 握手流程 ---
            System.out.println("Sending PING to master...");
            sendCommand(os, "PING");
            parser.readSimpleString();

            System.out.println("Sending REPLCONF listening-port...");
            sendCommand(os, "REPLCONF", "listening-port", String.valueOf(this.listeningPort));
            parser.readSimpleString();

            System.out.println("Sending REPLCONF capa psync2...");
            sendCommand(os, "REPLCONF", "capa", "psync2");
            parser.readSimpleString();

            System.out.println("Sending PSYNC...");
            sendCommand(os, "PSYNC", "?", "-1");
            parser.readSimpleString();

            // --- 处理 RDB 文件 ---
            System.out.println("Waiting for RDB file...");
            // 读取 '$' 符号
            is.read();
            StringBuilder lengthBuilder = new StringBuilder();
            int nextByte;
            while ((nextByte = is.read()) != '\r') {
                lengthBuilder.append((char) nextByte);
            }
            is.read(); // 跳过 '\n'
            int rdbLength = Integer.parseInt(lengthBuilder.toString());
            System.out.println("RDB file length: " + rdbLength);
            if (rdbLength > 0) {
                is.readNBytes(rdbLength);
                System.out.println("RDB file received and processed.");
            }
            // **关键修复**: 移除了之前错误的 is.read() 调用。
            // RDB 文件数据之后没有 \r\n，直接就是下一个命令。

            System.out.println("Handshake successful. Listening for propagated commands.");

            // --- 命令处理循环 ---
            while (!masterSocket.isClosed()) {
                CommandResult result = parser.readCommandWithCount();
                if (result == null || result.parts == null || result.parts.isEmpty()) {
                    break;
                }

                List<byte[]> commandParts = result.parts;
                String commandName = new String(commandParts.get(0), StandardCharsets.UTF_8).toUpperCase();

                if ("REPLCONF".equals(commandName) && commandParts.size() > 1
                        && "GETACK".equalsIgnoreCase(new String(commandParts.get(1), StandardCharsets.UTF_8))) {

                    System.out.println("Received REPLCONF GETACK *. Responding with ACK.");
                    sendCommand(os, "REPLCONF", "ACK", String.valueOf(bytesProcessed));
                    continue;
                }

                bytesProcessed += result.bytesRead;

                System.out.println("Received propagated command: " + commandName);
                List<byte[]> args = commandParts.subList(1, commandParts.size());
                Command command = this.commandHandler.getCommand(commandName);

                if (command != null) {
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
