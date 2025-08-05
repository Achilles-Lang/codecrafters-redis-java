package Controller;

import DAO.DataStore;
import DAO.ValueEntry;
import Utils.Protocol;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author Achilles
 * 接入层：负责接受和响应网络请求
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
                String key;

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
                        if (commandParts.size() < 3) {
                            outputStream.write("-ERR wrong number of arguments for 'set' command\r\n".getBytes());
                            break;
                        }
                        key = new String(commandParts.get(1), StandardCharsets.UTF_8);
                        byte[] value = commandParts.get(2);
                        long ttl = -1;
                        if (commandParts.size() > 4 && new String(commandParts.get(3), StandardCharsets.UTF_8).equalsIgnoreCase("PX")) {
                            ttl = Long.parseLong(new String(commandParts.get(4)));
                        }
                        // **修改点**: 调用新的 setString 方法，并传入 ValueEntry 对象
                        DataStore.setString(key, new ValueEntry(value, ttl));
                        outputStream.write("+OK\r\n".getBytes());
                        break;
                    case "GET":
                        if (commandParts.size() < 2) {
                            outputStream.write("-ERR wrong number of arguments for 'get' command\r\n".getBytes());
                            break;
                        }
                        key = new String(commandParts.get(1), StandardCharsets.UTF_8);
                        // **修改点**: 调用新的 getString 方法
                        ValueEntry entry = DataStore.getString(key);
                        if (entry != null) {
                            outputStream.write(('$' + String.valueOf(entry.value.length) + "\r\n").getBytes());
                            outputStream.write(entry.value);
                            outputStream.write("\r\n".getBytes());
                        } else {
                            outputStream.write("$-1\r\n".getBytes()); // Key 不存在、已过期或类型不匹配
                        }
                        break;
                    case "RPUSH":
                        if (commandParts.size() < 3) {
                            outputStream.write("-ERR wrong number of arguments for 'rpush' command\r\n".getBytes());
                            break;
                        }
                        key = new String(commandParts.get(1), StandardCharsets.UTF_8);
                        // RPUSH 支持一次添加多个值
                        List<byte[]> valuesToPush = commandParts.subList(2, commandParts.size());

                        int listSize = DataStore.rpush(key, valuesToPush);

                        if (listSize == -1) {
                            outputStream.write("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".getBytes());
                        } else {
                            // 返回 RESP 整数响应
                            outputStream.write((":" + listSize + "\r\n").getBytes());
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
