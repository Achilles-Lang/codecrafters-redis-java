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
    // ** ===> 关键修复 1: offset 初始化为 0，代表握手后处理的写命令字节数 <=== **
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

            performHandshake(os, parser);
            System.out.println("Handshake successful. Listening for propagated commands.");

            while (!masterSocket.isClosed()) {
                try {
                    // ** ===> 关键修复 2: 精确计算每个命令的大小 <=== **
                    long bytesBeforeCommand = parser.getBytesRead();
                    List<byte[]> commandParts = parser.readCommand();
                    if (commandParts == null || commandParts.isEmpty()) { break; }
                    long bytesAfterCommand = parser.getBytesRead();
                    long commandSize = bytesAfterCommand - bytesBeforeCommand;

                    String commandName = new String(commandParts.get(0), StandardCharsets.UTF_8).toLowerCase();
                    Command command = this.commandHandler.getCommand(commandName);

                    if (command != null) {
                        System.out.println("Executing propagated command: " + formatCommand(commandParts));
                        CommandContext context = new CommandContext(os, this.replicaOffset);

                        // ** ===> 关键修复 3: 只有写命令和 REPLCONF 需要执行 <=== **
                        // (其他命令如 PING 可以安全忽略)
                        if (command instanceof WriteCommand || commandName.equals("replconf")) {
                            command.execute(commandParts.subList(1, commandParts.size()), context);
                        }

                        // ** ===> 关键修复 4: 只有写命令才增加 offset <=== **
                        if (!commandName.equals("replconf")) {
                            this.replicaOffset += commandSize;
                        }
                    } else {
                        System.out.println("Unknown propagated command: " + commandName);
                        // 即使命令未知，也要累加偏移量以保持同步
                        this.replicaOffset += commandSize;
                    }
                } catch (Exception e) {
                    System.out.println("!!! Error executing propagated command: " + e.getMessage());
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

    private String formatCommand(List<byte[]> parts) {
        return parts.stream()
                .map(part -> new String(part, StandardCharsets.UTF_8))
                .collect(Collectors.joining(", ", "[", "]"));
    }
}
