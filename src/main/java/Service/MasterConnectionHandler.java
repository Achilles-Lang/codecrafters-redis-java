// 文件路径: src/main/java/Service/MasterConnectionHandler.java

package Service;

import Commands.Command;
import Commands.CommandHandler;
import Commands.CommandContext;
import Commands.WriteCommand;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Achilles
 * @date 2023/10/07
 * 主从复制连接处理类
 */
public class MasterConnectionHandler implements Runnable {
    private final String masterHost;
    private final int masterPort;
    private final int listeningPort;
    private final CommandHandler commandHandler;
    private long replicaOffset = 0;

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
            Protocol parser = new Protocol(is);

            // --- 阶段 1: 握手 ---
            performHandshake(os, parser);
            System.out.println("Handshake successful. Listening for propagated commands.");

            // ** ===> 关键修复：在这里初始化偏移量！ <=== **
            // 在握手完成后，从 parser 获取已经读取的字节数，并将其设置为我们的初始偏移量。
            this.replicaOffset = parser.getBytesRead();

            // --- 阶段 2: 命令传播循环 ---
            while (!masterSocket.isClosed()) {
                try {
                    // 记录进入循环前的偏移量，以便计算本次命令的真实大小
                    long bytesBeforeCommand = parser.getBytesRead();

                    List<byte[]> commandParts = parser.readCommand();
                    if (commandParts == null || commandParts.isEmpty()) {
                        break;
                    }

                    // 计算本次命令实际消耗的字节数
                    long commandSize = parser.getBytesRead() - bytesBeforeCommand;

                    String commandName = new String(commandParts.get(0), StandardCharsets.UTF_8).toLowerCase();
                    Command command = this.commandHandler.getCommand(commandName);

                    if (command != null) {
                        System.out.println("Executing propagated command: " + formatCommand(commandParts));

                        CommandContext context = new CommandContext(os, this.replicaOffset);

                        if (command instanceof WriteCommand || commandName.equals("replconf")) {
                            command.execute(commandParts.subList(1, commandParts.size()), context);
                        }

                        // 只为写命令更新偏移量
                        if (command instanceof WriteCommand) {
                            this.replicaOffset += commandSize;
                        }

                    } else {
                        System.out.println("Unknown propagated command: " + commandName);
                        this.replicaOffset += commandSize;
                    }
                } catch (Exception e) {
                    System.out.println("!!! Critical error executing propagated command: " + e.getMessage());
                    e.printStackTrace();
                }
            }

        } catch (IOException e) {
            System.out.println("Master connection lost: " + e.getMessage());
        }
    }

    // ... (performHandshake, sendCommand, formatCommand 等方法保持不变) ...
    private void performHandshake(OutputStream os, Protocol parser) throws IOException {
        sendCommand(os, "PING");
        parser.readSimpleString();
        sendCommand(os, "REPLCONF", "listening-port", String.valueOf(this.listeningPort));
        parser.readSimpleString();
        sendCommand(os, "REPLCONF", "capa", "psync2");
        parser.readSimpleString();
        sendCommand(os, "PSYNC", "?", "-1");
        String psyncResponse = parser.readSimpleString();
        if (psyncResponse == null || !psyncResponse.startsWith("FULLRESYNC")) {
            throw new IOException("Did not receive FULLRESYNC from master.");
        }
        parser.readRdbFile();
    }

    private void sendCommand(OutputStream os, String... args) throws IOException {
        StringBuilder cmd = new StringBuilder().append("*").append(args.length).append("\r\n");
        for (String arg : args) {
            cmd.append("$").append(arg.length()).append("\r\n").append(arg).append("\r\n");
        }
        os.write(cmd.toString().getBytes(StandardCharsets.UTF_8));
        os.flush();
    }

    private String formatCommand(List<byte[]> parts) {
        return parts.stream()
                .map(part -> new String(part, StandardCharsets.UTF_8))
                .collect(Collectors.joining(", ", "[", "]"));
    }
}
