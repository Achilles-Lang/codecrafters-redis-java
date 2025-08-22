package Service;

import Commands.Command;
import Commands.CommandHandler;
import Storage.DataStore;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

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

            System.out.println("Waiting for RDB file...");
            parser.readRdbFile();

            System.out.println("Handshake successful. Listening for propagated commands.");

            // --- 关键修复: 重写命令处理循环逻辑 ---
            while (!masterSocket.isClosed()) {
                CommandResult result = parser.readCommandWithCount();
                if (result == null || result.parts == null || result.parts.isEmpty()) {
                    break;
                }

                List<byte[]> commandParts = result.parts;
                String commandName = new String(commandParts.get(0), StandardCharsets.UTF_8).toUpperCase();

                boolean isGetAck = "REPLCONF".equals(commandName) && commandParts.size() > 1
                        && "GETACK".equalsIgnoreCase(new String(commandParts.get(1), StandardCharsets.UTF_8));

                if (isGetAck) {
                    // 如果是 GETACK, 立即用 *当前* 的偏移量回复
                    System.out.println("Received REPLCONF GETACK *. Responding with ACK.");
                    sendCommand(os, "REPLCONF", "ACK", String.valueOf(bytesProcessed));
                }

                // **无论是什么命令，都必须将它的字节数累加到偏移量中**
                bytesProcessed += result.bytesRead;

                // 如果命令不是 GETACK，则需要执行它 (PING, SET 等)
                if (!isGetAck) {
                    System.out.println("Received propagated command: " + commandName);
                    List<byte[]> args = commandParts.subList(1, commandParts.size());
                    Command command = this.commandHandler.getCommand(commandName);

                    if (command != null) {
                        command.execute(args, null);
                    } else {
                        System.out.println("Unknown propagated command: " + commandName);
                    }
                }

                if ("REPLCONF".equals(commandName) && commandParts.size() > 1
                        && "ACK".equalsIgnoreCase(new String(commandParts.get(1), StandardCharsets.UTF_8))) {

                    long offset = Long.parseLong(new String(commandParts.get(2), StandardCharsets.UTF_8));
                    DataStore.getInstance().processAck(offset);
                    // ACK 命令不需要增加偏移量，也不需要执行，直接继续
                    continue;
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
