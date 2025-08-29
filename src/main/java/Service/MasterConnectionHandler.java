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

            performHandshake(os, parser);
            System.out.println("Handshake successful. Listening for propagated commands.");

            while (!masterSocket.isClosed()) {
                try {
                    List<byte[]> commandParts = parser.readCommand();
                    if (commandParts == null || commandParts.isEmpty()) {
                        break;
                    }

                    String commandName = new String(commandParts.get(0), StandardCharsets.UTF_8).toLowerCase();

                    if ("replconf".equals(commandName) && commandParts.size() >= 2 && "getack".equalsIgnoreCase(new String(commandParts.get(1)))) {
                        // 直接用当前的 replicaOffset 回复，不经过 CommandHandler
                        String response = "*3\r\n" +
                                "$8\r\nREPLCONF\r\n" +
                                "$3\r\nACK\r\n" +
                                "$" + String.valueOf(this.replicaOffset).length() + "\r\n" +
                                this.replicaOffset + "\r\n";
                        os.write(response.getBytes(StandardCharsets.UTF_8));
                        os.flush();

                        // GETACK 命令本身不增加偏移量，因为它不是一个被复制的写命令
                        continue; // 处理完毕，进入下一次循环
                    }

                    long bytesBefore = this.replicaOffset;

                    Command command=this.commandHandler.getCommand(commandName);
                    if(command!=null){
                        System.out.println("Processing propagated command: " + formatCommand(commandParts));
                        if(command instanceof WriteCommand){
                            CommandContext dummyContext = new CommandContext(null);
                            command.execute(commandParts.subList(1,commandParts.size()),dummyContext);
                        }
                    }else {
                        System.out.println("Unknown propagated command:" + commandName);

                    }
                    this.replicaOffset = parser.getBytesRead() - bytesBefore;
                } catch (Exception e) {
                    System.out.println("!!! Error processing a propagated command, but continuing: " + e.getMessage());
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
