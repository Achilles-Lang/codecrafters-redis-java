// 文件路径: src/main/java/Service/MasterConnectionHandler.java

package Service;

import Commands.Command;
import Commands.CommandHandler;
import util.RdbUtil;

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

            sendCommand(os, "PING");
            String pongResponse = parser.readSimpleString();
            if (pongResponse == null || !pongResponse.equalsIgnoreCase("PONG")) {
                System.out.println("Error: Did not receive PONG from master.");
                return;
            }

            sendCommand(os, "REPLCONF", "listening-port", String.valueOf(this.listeningPort));
            parser.readSimpleString();
            sendCommand(os, "REPLCONF", "capa", "psync2");
            parser.readSimpleString();

            sendCommand(os, "PSYNC", "?", "-1");
            String psyncResponse = parser.readSimpleString();
            if (psyncResponse == null || !psyncResponse.startsWith("FULLRESYNC")) {
                System.out.println("Error: Did not receive FULLRESYNC from master.");
                return;
            }

            // **关键修复点**：现在 readRdbFile 应该能完整地读取 RDB 文件
            byte[] rdbFile = parser.readRdbFile();
            if (rdbFile != null) {
                System.out.println("Handshake successful. RDB file received. Listening for propagated commands.");
            } else {
                System.out.println("Error: Failed to read RDB file from master.");
                return;
            }

            // --- 命令处理循环 ---
            while (!masterSocket.isClosed()) {
                List<byte[]> commandParts = parser.readCommand();
                if (commandParts == null || commandParts.isEmpty()) {
                    break;
                }
                String commandName = new String(commandParts.get(0), StandardCharsets.UTF_8).toLowerCase();
                List<byte[]> args = commandParts.subList(1, commandParts.size());
                Command command = this.commandHandler.getCommand(commandName);
                if (command != null) {
                    command.execute(args, null);
                }
            }

        } catch (IOException e) {
            System.out.println("IOException in MasterConnectionHandler: " + e.getMessage());
        }
    }
    private void sendCommand(OutputStream os, String... args) throws IOException {
        StringBuilder cmd = new StringBuilder().append("*").append(args.length).append("\r\n");
        for (String arg : args) {
            cmd.append("$").append(arg.length()).append("\r\n").append(arg).append("\r\n");
        }
        os.write(cmd.toString().getBytes(StandardCharsets.UTF_8));
        os.flush();
    }
}