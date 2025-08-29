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

            // --- 阶段 2: 命令传播循环 ---
            System.out.println("Handshake successful. Listening for propagated commands.");

            while (!masterSocket.isClosed()) {
                try {
                    List<byte[]> commandParts = parser.readCommand();
                    if (commandParts == null || commandParts.isEmpty()) {
                        break;
                    }

                    // **关键修复 1**: 统一通过 CommandHandler 处理所有命令
                    String commandName = new String(commandParts.get(0), StandardCharsets.UTF_8).toLowerCase();
                    Command command = this.commandHandler.getCommand(commandName);

                    if (command != null) {
                        System.out.println("Executing propagated command: " + formatCommand(commandParts));

                        // **关键修复 2**: 创建一个非 null 的上下文，但内部的 stream 可以是 null
                        // 因为从节点执行命令时，不需要向 Master 回复执行结果（ACK 除外）
                        CommandContext context = new CommandContext(os,this.replicaOffset); // 传递 os 以便 REPLCONF ACK 可以回复

                        // 我们只执行写命令，因为读命令没有意义
                        if (command instanceof WriteCommand) {
                            command.execute(commandParts.subList(1, commandParts.size()), context);
                        } else if (commandName.equals("replconf")) {
                            // REPLCONF GETACK 是特例，它需要执行并回复
                            command.execute(commandParts.subList(1, commandParts.size()), context);
                        }

                        // **关键修复 3**: 更新已处理的字节偏移量
                        // 注意: 这假设 parser 能准确返回每个命令的字节数。
                        // 一个更精确的方法是在 Protocol 解析器中累加。
                        this.replicaOffset += calculateCommandSize(commandParts);

                    } else {
                        System.out.println("Unknown propagated command: " + commandName);
                    }
                } catch (Exception e) {
                    // **关键修复 4**: 捕获所有异常，防止单个命令错误导致整个复制中断
                    System.out.println("Error executing propagated command: " + e.getMessage());
                    e.printStackTrace();
                }
            }

        } catch (IOException e) {
            System.out.println("IOException in MasterConnectionHandler, connection lost: " + e.getMessage());
        }
    }

    private void performHandshake(OutputStream os, Protocol parser) throws IOException {
        // PING
        sendCommand(os, "PING");
        parser.readSimpleString();

        // REPLCONF listening-port
        sendCommand(os, "REPLCONF", "listening-port", String.valueOf(this.listeningPort));
        parser.readSimpleString();

        // REPLCONF capa
        sendCommand(os, "REPLCONF", "capa", "psync2");
        parser.readSimpleString();

        // PSYNC
        sendCommand(os, "PSYNC", "?", "-1");
        String psyncResponse = parser.readSimpleString();
        if (psyncResponse == null || !psyncResponse.startsWith("FULLRESYNC")) {
            throw new IOException("Did not receive FULLRESYNC from master.");
        }

        // 读取并忽略 RDB 文件 (在之后的阶段需要解析它)
        byte[] rdbData = parser.readRdbFile();
        // TODO: 在需要时，在这里添加 RDB 文件的解析逻辑
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
