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

            // ===> 核心修正 1: 在循环外部包裹一个大的 try-catch，处理连接级别的错误 <===
            while (!masterSocket.isClosed()) {
                // ===> 核心修正 2: 在循环内部包裹一个小的 try-catch，处理单个命令的错误 <===
                // 这就是我们的“防弹衣”，确保线程不会因为单个命令失败而死亡。
                try {
                    List<byte[]> commandParts = parser.readCommand();
                    if (commandParts == null || commandParts.isEmpty()) {
                        break; // 连接已关闭
                    }

                    String commandName = new String(commandParts.get(0), StandardCharsets.UTF_8).toLowerCase();
                    Command command = this.commandHandler.getCommand(commandName);

                    if (command != null) {
                        System.out.println("Executing propagated command: " + formatCommand(commandParts));
                        // 对于从节点来说，它只执行写命令，并且不需要回复。
                        // 我们创建一个临时的、无害的上下文。
                        if (command instanceof WriteCommand) {
                            CommandContext dummyContext = new CommandContext(null, false); // 使用可以接受 null 的构造函数
                            command.execute(commandParts.subList(1, commandParts.size()), dummyContext);
                        }
                    } else {
                        System.out.println("Unknown propagated command received: " + commandName);
                    }
                } catch (Exception e) {
                    // 如果单个命令执行失败，打印错误信息，但循环继续！
                    System.out.println("!!! Error executing a propagated command, but continuing loop. Error: " + e.getMessage());
                    e.printStackTrace(); // 打印详细的堆栈信息以帮助调试
                }
            }
        } catch (IOException e) {
            // 这个 catch 块只处理网络连接问题
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

