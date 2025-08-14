package Service;

import Commands.Command;
import Commands.CommandHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

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
                List<byte[]> commandParts=protocol.readCommand();
                if(commandParts == null){
                    break;
                }

                String commandName=new String(commandParts.get(0), StandardCharsets.UTF_8).toLowerCase();

                if(inTransaction){
                    //如果在事务中
                    if("exec".equals(commandName)||"discard".equals(commandName)){
                        inTransaction=false;
                        transactionQueue.clear();
                        outputStream.write("+OK\r\n".getBytes());
                    } else if ("multi".equals(commandName)) {
                        outputStream.write("-ERR MULTI calls can not be nested\r\n".getBytes());
                    }else {
                        transactionQueue.add(commandParts);
                        outputStream.write("+QUEUED\r\n".getBytes());
                    }
                }else {
                    if("multi".equals(commandName)){
                        inTransaction=true;
                        transactionQueue.clear();
                        outputStream.write("+OK\r\n".getBytes());
                    }else {
                        List<byte[]> args=commandParts.subList(1, commandParts.size());

                        Command command=commandHandler.getCommand(commandName);
                        Object result=(command==null)
                                ? new Exception("unknown command '" + commandName + "'")
                                :command.execute(args);
                        RespEncoder.encode(outputStream, result);
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
    //用于将Java对象编码成RESP字符串
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
}
