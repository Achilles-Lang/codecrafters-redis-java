package Service;

import Commands.Command;
import Commands.CommandContext;
import Commands.CommandHandler;
import Commands.Impl.ConfigCommand;
import Commands.Impl.PingCommand;
import Commands.Impl.SubscribeCommand;
import Commands.WriteCommand;
import Commands.Impl.BlpopCommand;
import Storage.DataStore;
import util.RdbUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Achilles
 * 接入层：负责接受和响应网络请求
 */
public class ClientHandler implements Runnable{

    private final Socket clientSocket;
    private final CommandHandler commandHandler;
    private boolean inTransaction = false;
    private final Queue<List<byte[]>> transactionQueue=new LinkedList<>();
    private boolean isSubscribed = false;
    // 订阅模式下命令白名单
    private static final Set<String> ALLOWED_SUBSCRIBE_COMMANDS = new HashSet<>(Arrays.asList(
            "subscribe", "unsubscribe", "psubscribe", "punsubscribe", "ping", "quit"
    ));


    public ClientHandler(Socket socket, CommandHandler commandHandler) {
        this.clientSocket = socket;
        this.commandHandler = commandHandler;
    }

    public Socket getClientSocket() {
        return clientSocket;
    }

    @Override
    public void run( ) {
        //获取该连接的输入和输出流
        OutputStream outputStream = null;
        try (Socket socket = this.clientSocket) {
            outputStream = socket.getOutputStream();
            Protocol protocol = new Protocol(socket.getInputStream());

            while (!socket.isClosed()) {
                List<byte[]> commandParts = protocol.readCommand();
                if (commandParts == null || commandParts.isEmpty()) {
                    break;
                }
                String commandName = new String(commandParts.get(0), StandardCharsets.UTF_8).toLowerCase();
                String lowerCaseCommandName = commandName.toLowerCase();

                if(isSubscribed){
                    if(!ALLOWED_SUBSCRIBE_COMMANDS.contains(lowerCaseCommandName)){
                        String errorMsg = "Can't execute '" + lowerCaseCommandName + "'";
                       RespEncoder.encode(outputStream, new Exception(errorMsg));
                       outputStream.flush();
                       continue;
                    }
                }
                if ("REPLCONF".equalsIgnoreCase(commandName) && commandParts.size() > 2
                        && "ACK".equalsIgnoreCase(new String(commandParts.get(1), StandardCharsets.UTF_8))) {

                    long offset = Long.parseLong(new String(commandParts.get(2), StandardCharsets.UTF_8));
                    DataStore.getInstance().processAck(offset);
                    continue; // ACK processed, continue to next command
                }

                if ("REPLCONF".equalsIgnoreCase(commandName) && commandParts.size() > 2
                        && "GETACK".equalsIgnoreCase(new String(commandParts.get(1), StandardCharsets.UTF_8))
                        && "*".equals(new String(commandParts.get(2), StandardCharsets.UTF_8))) {

                    System.out.println("Received REPLCONF GETACK *. Responding with ACK.");

                    // 在这里获取当前偏移量。因为还没有处理任何写命令，所以值应该为 0。
                    long offset = DataStore.getInstance().getReplicaOffset();

                    // 构造并发送 REPLCONF ACK <offset> 响应
                    List<byte[]> ackResponse = new ArrayList<>();
                    ackResponse.add("REPLCONF".getBytes(StandardCharsets.UTF_8));
                    ackResponse.add("ACK".getBytes(StandardCharsets.UTF_8));
                    ackResponse.add(String.valueOf(offset).getBytes(StandardCharsets.UTF_8));

                    RespEncoder.encode(outputStream, ackResponse);
                    outputStream.flush();
                    continue;
                }

                // **KEY FIX 2**: Convert to lowercase *after* the ACK check
                if (inTransaction) {
                    //如果在事务中
                    if ("exec".equals(commandName)) {
                        List<Object> results = new LinkedList<>();
                        CommandContext context = new CommandContext(outputStream,this.isSubscribed);

                        for (List<byte[]> queuedCommandParts : transactionQueue) {
                            String queuedCommandName = new String(queuedCommandParts.get(0), StandardCharsets.UTF_8);
                            List<byte[]> queuedArgs = queuedCommandParts.subList(1, queuedCommandParts.size());

                            Command commandToExecute = commandHandler.getCommand(queuedCommandName);

                            if (commandToExecute != null) {
                                results.add(commandToExecute.execute(queuedArgs, context));

                            } else {
                                results.add(new Exception("unknown command '" + queuedCommandName + "'"));
                            }
                        }
                        RespEncoder.encode(outputStream, results);
                        transactionQueue.clear();
                        inTransaction = false;

                    } else if ("discard".equals(commandName)) {
                        transactionQueue.clear();
                        inTransaction = false;
                        RespEncoder.encode(outputStream, "OK");
                    } else if ("multi".equals(commandName)) {
                        RespEncoder.encode(outputStream, new Exception("MULTI calls can not be nested"));
                    } else {
                        transactionQueue.add(commandParts);
                        RespEncoder.encode(outputStream, "QUEUED");
                    }
                } else {
                    if ("multi".equals(lowerCaseCommandName)) {
                        inTransaction = true;
                        transactionQueue.clear();
                        RespEncoder.encode(outputStream, "OK");
                    } else if ("exec".equals(lowerCaseCommandName) || "discard".equals(lowerCaseCommandName)) {
                        RespEncoder.encode(outputStream, new Exception(lowerCaseCommandName.toUpperCase() + " without MULTI"));

                    } else {
                        List<byte[]> args = commandParts.subList(1, commandParts.size());
                        Command command = commandHandler.getCommand(lowerCaseCommandName);

                        if (command == null) {
                            RespEncoder.encode(outputStream, new Exception("unknown command '" + commandName + "'"));
                        } else {
                            CommandContext context = new CommandContext(outputStream,this.isSubscribed);

                            if (command instanceof WriteCommand) {
                                long commandSize = calculateCommandSize(commandParts);

                                DataStore.getInstance().incrementReplicaOffset(commandSize);
                                DataStore.getInstance().propagateCommand(commandParts);
                            }
                            Object result = command.execute(args, context);

                            if (result == Command.STATE_CHANGE_SUBSCRIBE) {
                                this.isSubscribed = true;
                            } else if (result != BlpopCommand.RESPONSE_ALREADY_SENT) {
                                if (result instanceof FullResyncResponse) {
                                    FullResyncResponse resync = (FullResyncResponse) result;
                                    String fullResyncLine = "+FULLRESYNC " + resync.getMasterReplid() + " " + resync.getMasterReplOffset() + "\r\n";
                                    outputStream.write(fullResyncLine.getBytes(StandardCharsets.UTF_8));
                                    byte[] rdbFile = RdbUtil.getEmptyRdbFile();
                                    outputStream.write(("$" + rdbFile.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                                    outputStream.write(rdbFile);
                                } else {
                                    RespEncoder.encode(outputStream, result);
                                }
                            }
                        }
                    }
                }
                outputStream.flush();
            }
        } catch (IOException e) {
            System.out.println("Connection closed for " + clientSocket.getRemoteSocketAddress() + ": " + e.getMessage());
        } finally {
            DataStore.getInstance().unsubscribeClient(outputStream);
            System.out.println("Client handler thread finished for " + clientSocket.getRemoteSocketAddress());

        }

    }
        //计算这个命令的RESP字节长度
    private long calculateCommandSize(List<byte[]> commandParts) {
        if (commandParts == null || commandParts.isEmpty()) {
            return 0;
        }

        long totalSize = 0;
        // RESP数组头: *<number-of-elements>\r\n
        totalSize += 1 + String.valueOf(commandParts.size()).length() + 2;

        for (byte[] part : commandParts) {
            if (part != null) {
                // RESP批量字符串: $<length>\r\n<data>\r\n
                totalSize += 1 + String.valueOf(part.length).length() + 2 + part.length + 2;
            } else {
                // RESP批量字符串null: $-1\r\n
                totalSize += 4;
            }
        }

        return totalSize;
    }


    //已迁移到 RespEncoder
    private void sendResponse(OutputStream outputStream, Object result) throws IOException {
        if (result == null) {
            outputStream.write("$-1\r\n".getBytes()); // NIL Bulk String
        } else if (result instanceof String) {
            outputStream.write(("+" + result + "\r\n").getBytes()); // Simple String
        } else if (result instanceof byte[]) {
            byte[] arr = (byte[]) result;
            outputStream.write(('$' + String.valueOf(arr.length) + "\r\n").getBytes());
            outputStream.write(arr);
            outputStream.write("\r\n".getBytes());
        } else if (result instanceof Long || result instanceof Integer) {
        outputStream.write((":" + result + "\r\n").getBytes()); // Integer
        } else if (result instanceof List) {
        // ... 编码数组的逻辑 ...
        } else if (result instanceof Exception) {
            outputStream.write(("-ERR " + ((Exception) result).getMessage() + "\r\n").getBytes()); // Error
        }
    }
    /**
     * 新增一个辅助方法，用于将命令部分列表编码为 RESP 数组并写入输出流。
     * @param os 输出流
     * @param parts 命令的各个部分 (e.g., "SET", "key", "value")
     * @throws IOException
     */
    private void writeRespArray(OutputStream os, List<byte[]> parts) throws IOException {
        // 写入数组头, e.g., *3\r\n
        os.write(("*" + parts.size() + "\r\n").getBytes(StandardCharsets.UTF_8));

        // 依次写入每个部分
        for (byte[] part : parts) {
            // 写入 Bulk String 头, e.g., $3\r\n
            os.write(("$" + part.length + "\r\n").getBytes(StandardCharsets.UTF_8));
            // 写入数据本身
            os.write(part);
            // 写入结尾的 CRLF
            os.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
    }
}

