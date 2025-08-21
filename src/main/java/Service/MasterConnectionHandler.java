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
import java.util.function.LongUnaryOperator;

/**
 * @author Achilles
 * 作为一个客户端，处理与主节点的连接和通信
 */
public class MasterConnectionHandler implements Runnable {
    private final String masterHost;
    private final int masterPort;
    private final int listeningPort;
    private final CommandHandler commandHandler;
    // 新增：用于追踪处理的字节数，作为复制偏移量
    private long bytesProcessed = 0L;
    private long processedBytes=0L;

    public MasterConnectionHandler(String host, int port, int listeningPort, CommandHandler commandHandler) {
        this.masterHost = host;
        this.masterPort = port;
        this.listeningPort = listeningPort;
        this.commandHandler = commandHandler;
    }

    // 在 MasterConnectionHandler.java 中，用这个版本替换掉旧的 run 方法
// 在 Service/MasterConnectionHandler.java 中
    @Override
    public void run() {
        try (Socket masterSocket = new Socket(masterHost, masterPort)) {
            OutputStream os = masterSocket.getOutputStream();
            InputStream is = masterSocket.getInputStream();

            // ... (握手逻辑 PING, REPLCONF, PSYNC, readRdbFile 保持不变) ...
            System.out.println("Handshake successful. Listening for propagated commands.");

            // --- 命令处理循环 ---
            Protocol commandParser = new Protocol(is);
            while (!masterSocket.isClosed()) {
                List<byte[]> commandParts = commandParser.readCommand();
                if (commandParts == null || commandParts.isEmpty()) {
                    break;
                }

                // **关键修复点**
                // 1. 先解析出命令名和参数
                String commandName = new String(commandParts.get(0), StandardCharsets.UTF_8).toLowerCase();
                List<byte[]> args = commandParts.subList(1, commandParts.size()); // <-- 在这里一次性声明和初始化 args

                // 2. 然后再使用 commandName 和 args 进行判断
                if ("replconf".equals(commandName) && !args.isEmpty() && "getack".equalsIgnoreCase(new String(args.get(0)))) {
                    // 回复 GETACK
                    sendCommand(os, "REPLCONF", "ACK", String.valueOf(this.processedBytes));
                    System.out.println("Responded to GETACK with offset: " + this.processedBytes);
                } else {
                    // 处理普通传播命令
                    long commandSize = calculateRespSize(commandParts);

                    Command command = this.commandHandler.getCommand(commandName);
                    if (command != null) {
                        // 这里使用的 args 就是上面声明的那个
                        command.execute(args, null);
                    }

                    // 累加已处理的字节数
                    this.processedBytes += commandSize;
                }
            }

        } catch (IOException e) {
            System.out.println("IOException in MasterConnectionHandler: " + e.getMessage());
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

    public long getProcessedBytes() {
        return processedBytes;
    }
    private long calculateRespSize(List<byte[]> parts){
        long size = 0;
        size += ("*" + parts.size() + "\r\n").getBytes().length;
        for (byte[] part:parts){
            size+=("$" + part.length + "\r\n").getBytes().length;
            size+=part.length;
            size+=2;
        }
        return size;
    }
}
