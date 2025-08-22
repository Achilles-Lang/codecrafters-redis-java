package Service;

import Commands.Command;
import Commands.CommandHandler;
import Commands.WriteCommand;
import Storage.DataStore;
import util.RdbUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
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
        try (Socket socket=this.clientSocket) {
            OutputStream outputStream = socket.getOutputStream();
            Protocol protocol=new Protocol(socket.getInputStream());

            while (!socket.isClosed()) {
                List<byte[]> commandParts= (List<byte[]>) protocol.readCommand();
                if(commandParts == null || commandParts.isEmpty()){
                    break;
                }
                String commandName=new String(commandParts.get(0), StandardCharsets.UTF_8).toLowerCase();

                if ("REPLCONF".equals(commandName) && commandParts.size() > 2
                        && "ACK".equalsIgnoreCase(new String(commandParts.get(1), StandardCharsets.UTF_8))) {

                    long offset = Long.parseLong(new String(commandParts.get(2), StandardCharsets.UTF_8));
                    DataStore.getInstance().processAck(offset);

                    // ACK 是一个内部响应，不需要进一步处理，直接继续下一次循环
                    continue;
                }

                if(inTransaction){
                    //如果在事务中
                    if("exec".equals(commandName)){
                        List<Object> results = new LinkedList<>();
                        for (List<byte[]> queuedCommandParts : transactionQueue) {
                            String queuedCommandName = new String(queuedCommandParts.get(0), StandardCharsets.UTF_8);
                            List<byte[]> queuedArgs = queuedCommandParts.subList(1, queuedCommandParts.size());

                            Command commandToExecute = commandHandler.getCommand(queuedCommandName);

                            if(commandToExecute!=null){
                                results.add(commandToExecute.execute(queuedArgs, outputStream));

                            }else{
                                results.add(new Exception("unknown command '" + queuedCommandName + "'"));
                            }
                        }
                        RespEncoder.encode(outputStream,results);
                        transactionQueue.clear();
                        inTransaction=false;

                    } else if ( "discard".equals(commandName)) {
                        transactionQueue.clear();
                        inTransaction=false;
                        RespEncoder.encode(outputStream,"OK");
                    } else if ("multi".equals(commandName)) {
                        RespEncoder.encode(outputStream, new Exception("MULTI calls can not be nested"));
                    }else{
                        transactionQueue.add(commandParts);
                        RespEncoder.encode(outputStream, "QUEUED");
                    }
                }else {
                    if("multi".equals(commandName)){
                        inTransaction=true;
                        transactionQueue.clear();
                        RespEncoder.encode(outputStream, "OK");
                    } else if ("exec".equals(commandName)||"discard".equals(commandName)) {
                        RespEncoder.encode(outputStream, new Exception(commandName.toUpperCase() + " without MULTI"));

                    } else {
                        List<byte[]> args=commandParts.subList(1, commandParts.size());
                        Command command=commandHandler.getCommand(commandName);

                        if(command==null){
                            RespEncoder.encode(outputStream, new Exception("unknown command '" + commandName + "'"));
                        }else{
                            if(command instanceof WriteCommand){
                                DataStore.getInstance().propagateCommand(commandParts);
                            }
                            Object result=command.execute(args, outputStream);

                            if(result instanceof FullResyncResponse){
                                FullResyncResponse resync=(FullResyncResponse)result;
                                String fullResyncLine="+FULLRESYNC "+resync.getMasterReplid()+" "+resync.getMasterReplOffset()+"\r\n";
                                outputStream.write(fullResyncLine.getBytes(StandardCharsets.UTF_8));
                                byte[] rdbFile=RdbUtil.getEmptyRdbFile();
                                outputStream.write(("$" + rdbFile.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                                outputStream.write(rdbFile);
                            } else {
                                RespEncoder.encode(outputStream, result);
                            }
                        }
                    }
                }
                outputStream.flush();
            }
        } catch (IOException e) {
            System.out.println("Connection closed for " + clientSocket.getRemoteSocketAddress() + ": " + e.getMessage());
        }finally {
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

