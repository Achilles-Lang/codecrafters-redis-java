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

            // --- 阶段 2: 命令传播循环 ---
            while (!masterSocket.isClosed()) {
                // ** ===> 关键修复 1: 为循环的每一次迭代都加上独立的 try-catch 块 <=== **
                try {
                    List<byte[]> commandParts = parser.readCommand();
                    if (commandParts == null || commandParts.isEmpty()) {
                        break; // 连接已关闭
                    }

                    String commandName = new String(commandParts.get(0), StandardCharsets.UTF_8).toLowerCase();
                    Command command = this.commandHandler.getCommand(commandName);

                    if (command != null) {
                        System.out.println("Executing propagated command: " + formatCommand(commandParts));

                        // ** ===> 关键修复 2: 创建一个有效的、非 null 的上下文 <=== **
                        // 我们需要一个可以传递 offset 的构造函数
                        CommandContext context = new CommandContext(os, this.replicaOffset);

                        // 只执行写命令或需要回复的 REPLCONF 命令
                        if (command instanceof WriteCommand || commandName.equals("replconf")) {
                            command.execute(commandParts.subList(1, commandParts.size()), context);
                        }

                        // 只为非 REPLCONF GETACK 的写命令更新偏移量
                        if (!commandName.equals("replconf")) {
                            this.replicaOffset += calculateCommandSize(commandParts);
                        }

                    } else {
                        System.out.println("Unknown propagated command: " + commandName);
                        this.replicaOffset += calculateCommandSize(commandParts); // 即使未知，也要累加偏移量
                    }
                } catch (Exception e) {
                    // ** ===> 关键修复 3: 捕获所有异常，打印堆栈，防止线程死亡 <=== **
                    System.out.println("!!! Critical error executing propagated command, but replication will continue. Error: " + e.getMessage());
                    e.printStackTrace();
                }
            }

        } catch (IOException e) {
            System.out.println("Master connection lost: " + e.getMessage());
        }
    }

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

    private long calculateCommandSize(List<byte[]> parts) {
        long size = 0;
        size += ("*" + parts.size() + "\r\n").getBytes().length;
        for (byte[] part : parts) {
            size += ("$" + part.length + "\r\n").getBytes().length;
            size += part.length;
            size += "\r\n".getBytes().length;
        }
        return size;
    }

    private String formatCommand(List<byte[]> parts) {
        return parts.stream()
                .map(part -> new String(part, StandardCharsets.UTF_8))
                .collect(Collectors.joining(", ", "[", "]"));
    }
}
