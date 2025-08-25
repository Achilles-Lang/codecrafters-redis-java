// 文件路径: src/main/java/Service/MasterConnectionHandler.java

package Service;

import Commands.Command;
import Commands.CommandHandler;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

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
    private long processedBytes = 0;

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

            // --- 阶段 1: PING ---
            sendCommand(os, "PING");
            String pongResponse = parser.readSimpleString();
            if (pongResponse == null || !pongResponse.equalsIgnoreCase("PONG")) {
                System.out.println("Error: Did not receive PONG from master.");
                return;
            }
            processedBytes+=6;

            // --- 阶段 2: REPLCONF ---
            sendCommand(os, "REPLCONF", "listening-port", String.valueOf(this.listeningPort));
            parser.readSimpleString();
            sendCommand(os, "REPLCONF", "capa", "psync2");
            parser.readSimpleString();
            processedBytes+=6;

            // --- 阶段 3: PSYNC ---
            sendCommand(os, "PSYNC", "?", "-1");
            String psyncResponse = parser.readSimpleString();
            if (psyncResponse == null || !psyncResponse.startsWith("FULLRESYNC")) {
                System.out.println("Error: Did not receive FULLRESYNC from master.");
                return;
            }
            processedBytes+=psyncResponse.length()+2;

            // 读取 RDB 文件
            byte[] rdbData=parser.readRdbFile();
            processedBytes += ("$" + rdbData.length + "\r\n").getBytes(StandardCharsets.UTF_8).length;
            processedBytes += rdbData.length;
            System.out.println("Handshake successful. Listening for propagated commands.");

            // --- 命令处理循环 ---
            while (!masterSocket.isClosed()) {
                List<byte[]> commandParts = parser.readCommand();
                if (commandParts == null || commandParts.isEmpty()) {
                    break;
                }
                long commandLength=0;
                System.out.println("Received command: " + formatCommand(commandParts));

                String commandName = new String(commandParts.get(0), StandardCharsets.UTF_8).toLowerCase();

                if(commandName.equals("replconf")){
                    System.out.println("Command is REPLCONF, checking subcommands.");

                    if (commandParts.size() == 3 && new String(commandParts.get(1)).equalsIgnoreCase("GETACK") && new String(commandParts.get(2)).equals("*")) {
                        long offset=processedBytes;
                        String response = "*3\r\n" +
                                "$8\r\nREPLCONF\r\n" +
                                "$3\r\nACK\r\n" +
                                "$" + String.valueOf(offset).length() + "\r\n" +
                                offset + "\r\n";

                        os.write(response.getBytes(StandardCharsets.UTF_8));
                        System.out.println("Sent REPLCONF ACK " + offset);
                    }else{
                        System.out.println("REPLCONF command but not GETACK, size: " + commandParts.size());
                        System.out.println("Arg 1: " + new String(commandParts.get(1), StandardCharsets.UTF_8));
                        if(commandParts.size()>2){
                            System.out.println("Arg 2: " + new String(commandParts.get(2), StandardCharsets.UTF_8));

                        }
                    }
                } else if (commandName.equals("ping")) {
                    System.out.println("Received PING from master. No further action needed.");


                } else {


                    List<byte[]> args = commandParts.subList(1, commandParts.size());
                    Command command = this.commandHandler.getCommand(commandName);
                    if (command != null) {
                        command.execute(args, null);
                    }
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
    private String formatCommand(List<byte[]> parts) {
        return parts.stream()
                .map(part -> new String(part, StandardCharsets.UTF_8))
                .collect(java.util.stream.Collectors.joining(", ", "[", "]"));
    }
}