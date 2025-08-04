import javax.swing.text.html.parser.Parser;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author Achilles
 */
public class ClientHandler implements Runnable{

    private final Socket clientSocket;

    public ClientHandler(Socket socket) {
        this.clientSocket = socket;
    }

    public Socket getClientSocket() {
        return clientSocket;
    }

    @Override
    public void run( ) {
        //获取该连接的输入和输出流
        try (Socket socket=this.clientSocket) {
            System.out.println("New client handler thread started for " + socket.getRemoteSocketAddress());

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
                String commandName=new String(commandParts.get(0), StandardCharsets.UTF_8).toUpperCase();

                //3.根据命令名称执行不同的操作
                switch (commandName){
                    case "PING":
                        outputStream.write("+PONG\r\n".getBytes());
                        break;
                    case "ECHO":
                        if(commandParts.size()>1){
                            byte[] argument = commandParts.get(1);
                            //回复一个批量字符串：$<length>\r\n<data>\r\n
                            outputStream.write(('$' + String.valueOf(argument.length) + "\r\n").getBytes());
                            outputStream.write(argument);
                            outputStream.write("\r\n".getBytes());
                        }else{
                            //参数不足
                            outputStream.write("-ERR wrong number of arguments for 'echo' command\r\n".getBytes());
                        }
                        break;
                    case "SET":
                        if (commandParts.size() > 2) {
                            String key = new String(commandParts.get(1), StandardCharsets.UTF_8);
                            byte[] value = commandParts.get(2);
                            DataStore.set(key, value);
                            outputStream.write("+OK\r\n".getBytes());
                        } else {
                            outputStream.write("-ERR wrong number of arguments for 'set' command\r\n".getBytes());
                        }
                        break;
                    case "GET":
                        if (commandParts.size() > 1) {
                            String key = new String(commandParts.get(1), StandardCharsets.UTF_8);
                            byte[] value = DataStore.get(key);
                            if (value != null) {
                                // 找到了值，以 Bulk String 格式返回
                                outputStream.write(('$' + String.valueOf(value.length) + "\r\n").getBytes());
                                outputStream.write(value);
                                outputStream.write("\r\n".getBytes());
                            } else {
                                // 没找到值，返回 Null Bulk String
                                outputStream.write("$-1\r\n".getBytes());
                            }
                        } else {
                            outputStream.write("-ERR wrong number of arguments for 'get' command\r\n".getBytes());
                        }
                        break;
                    default:
                        //不支持的命令
                        outputStream.write(("-ERR unknown command '" + commandName + "'\r\n").getBytes());
                        break;
                }
                outputStream.flush();
            }

        } catch (IOException e) {
            System.out.println("Connection closed for " + clientSocket.getRemoteSocketAddress() + ": " + e.getMessage());
        }finally {
            System.out.println("Client handler thread finished for " + clientSocket.getRemoteSocketAddress());

        }

    }


}
