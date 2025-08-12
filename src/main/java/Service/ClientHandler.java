package Service;

import Commands.Command;
import Commands.CommandHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

/**
 * @author Achilles
 * 接入层：负责接受和响应网络请求
 */
public class ClientHandler implements Runnable{

    private final Socket clientSocket;
    private final CommandHandler commandHandler;

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
                //1.使用解析器读取一个完整的命令
                List<byte[]> commandParts=protocol.readCommand();
                if(commandParts == null){
                    //客户端关闭了连接
                    break;
                }
                //2.解析命令名称
                String commandName=new String(commandParts.get(0));
                List<byte[]> args=commandParts.subList(1, commandParts.size());

                Command command=commandHandler.getCommand(commandName);

                if(command==null) {
                    outputStream.write(("-ERR unknown command '" + commandName + "'\r\n").getBytes());
                }else{
                    Object result=command.execute(args);
                    sendResponse(outputStream,result);
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
    private void sendResponse(OutputStream outputStream, Object result) throws IOException {if (result == null) {
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
