package Controller;

import Config.WrongTypeException;
import DAO.DataStore;
import DAO.ValueEntry;
import DAO.*;
import Utils.Protocol;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                        if (commandParts.size() > 4) {
                            if (new String(commandParts.get(3), StandardCharsets.UTF_8).equalsIgnoreCase("PX")) {
                                ttl = Long.parseLong(new String(commandParts.get(4)));
                            }
                        }
                        // **修改点**: 调用新的 setString 方法，并传入 ValueEntry 对象
                        long expiryTimestamp = (ttl > 0) ? (System.currentTimeMillis() + ttl) : -1;

                        // 将计算好的绝对时间戳传入 ValueEntry
                        DataStore.setString(key, new ValueEntry(value, expiryTimestamp));

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
                        List<byte[]> valuesToRPush = commandParts.subList(2, commandParts.size());

                        int rPushListSize = DataStore.rpush(key, valuesToRPush);

                        if ( rPushListSize== -1) {
                            outputStream.write("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".getBytes());
                        } else {
                            // 返回 RESP 整数响应
                            outputStream.write((":" + rPushListSize + "\r\n").getBytes());
                        }
                        break;
                    case "LPUSH":
                        if (commandParts.size() < 3) {
                            outputStream.write("-ERR wrong number of arguments for 'lpush' command\r\n".getBytes());
                            break;
                        }
                        try {
                            key = new String(commandParts.get(1), StandardCharsets.UTF_8);
                            // LPUSH 也支持一次添加多个值
                            List<byte[]> valuesToLPush = commandParts.subList(2, commandParts.size());

                            int lPushListSize = DataStore.lpush(key, valuesToLPush);

                            if (lPushListSize == -1) {
                                outputStream.write("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".getBytes());
                            } else {
                                // 返回 RESP 整数响应
                                outputStream.write((":" + lPushListSize + "\r\n").getBytes());
                            }
                        } catch (Exception e) {
                            // 捕获通用异常以防万一
                            outputStream.write(("-ERR " + e.getMessage() + "\r\n").getBytes());
                        }
                        break;
                    case "LPOP":
                        if (commandParts.size() < 2 || commandParts.size() > 3) {
                            outputStream.write("-ERR wrong number of arguments for 'lpop' command\r\n".getBytes());
                            break;
                        }
                        try {
                            key = new String(commandParts.get(1), StandardCharsets.UTF_8);
                            int count=1;

                            final boolean countProvided=commandParts.size()==3;

                            if(countProvided){
                                count=Integer.parseInt(new String(commandParts.get(2)));
                                if(count < 0){
                                    outputStream.write("-ERR value is out of range, must be positive\r\n".getBytes());
                                    break;
                                }
                            }

                            List<byte[]> poppedValues = DataStore.lpop(key, count);

                            if(poppedValues==null){
                                outputStream.write("$-1\r\n".getBytes());
                            }else if(!countProvided){
                                if(poppedValues.isEmpty()){
                                    outputStream.write("$-1\r\n".getBytes());
                                }else {
                                    //key存在
                                    byte[] singleValue = poppedValues.get(0);
                                    outputStream.write(('$' + String.valueOf(singleValue.length) + "\r\n").getBytes());
                                    outputStream.write(singleValue);
                                    outputStream.write("\r\n".getBytes());
                            }
                            }else{
                                outputStream.write(("*" + poppedValues.size() + "\r\n").getBytes());
                                for (byte[] element : poppedValues) {
                                    outputStream.write(('$' + String.valueOf(element.length) + "\r\n").getBytes());
                                    outputStream.write(element);
                                    outputStream.write("\r\n".getBytes());
                            }
                                }
                        } catch (WrongTypeException e) {
                            outputStream.write(("-"+e.getMessage()+"\r\n").getBytes());
                        }
                        break;
                    case "BLPOP":
                        if (commandParts.size() != 3) {
                            outputStream.write("-ERR wrong number of arguments for 'blpop' command\r\n".getBytes());
                            break;
                        }
                        try {
                            key = new String(commandParts.get(1), StandardCharsets.UTF_8);
                            double timeoutSeconds = Double.parseDouble(new String(commandParts.get(2)));

                            byte[] poppedValue = DataStore.blpop(key, timeoutSeconds);

                            if (poppedValue == null) {
                                // 超时或 key 不存在，返回 NIL Bulk String
                                outputStream.write("$-1\r\n".getBytes());
                            } else {
                                // 成功弹出，返回一个包含 key 和 value 的二元数组
                                outputStream.write("*2\r\n".getBytes());
                                // 1. 返回 key
                                byte[] keyBytes = commandParts.get(1);
                                outputStream.write(('$' + String.valueOf(keyBytes.length) + "\r\n").getBytes());
                                outputStream.write(keyBytes);
                                outputStream.write("\r\n".getBytes());
                                // 2. 返回 value
                                outputStream.write(('$' + String.valueOf(poppedValue.length) + "\r\n").getBytes());
                                outputStream.write(poppedValue);
                                outputStream.write("\r\n".getBytes());
                            }

                        } catch (NumberFormatException e) {
                            outputStream.write("-ERR value is not an integer or out of range\r\n".getBytes());
                        } catch (WrongTypeException e) {
                            outputStream.write(("-"+e.getMessage()+"\r\n").getBytes());
                        } catch (InterruptedException e) {
                            // 线程被中断，可以简单返回 NIL
                            outputStream.write("$-1\r\n".getBytes());
                            Thread.currentThread().interrupt(); // 恢复中断状态
                        }
                        break;
                    case "LRANGE":
                        if (commandParts.size() != 4) {
                            outputStream.write("-ERR wrong number of arguments for 'lrange' command\r\n".getBytes());
                            break;
                        }
                        try {
                            key = new String(commandParts.get(1), StandardCharsets.UTF_8);
                            int start = Integer.parseInt(new String(commandParts.get(2)));
                            int end = Integer.parseInt(new String(commandParts.get(3)));

                            // 调用 DataStore 的核心逻辑
                            List<byte[]> result = DataStore.lrange(key, start, end);

                            // 将返回的 List<byte[]> 格式化为 RESP Array
                            outputStream.write(("*" + result.size() + "\r\n").getBytes());
                            for (byte[] element : result) {
                                outputStream.write(('$' + String.valueOf(element.length) + "\r\n").getBytes());
                                outputStream.write(element);
                                outputStream.write("\r\n".getBytes());
                            }

                        } catch (NumberFormatException e) {
                            outputStream.write("-ERR value is not an integer or out of range\r\n".getBytes());
                        } catch (WrongTypeException e) {
                            outputStream.write(("-"+e.getMessage()+"\r\n").getBytes());
                        }
                        break;

                    case "LLEN":
                        if (commandParts.size() != 2) {
                            outputStream.write("-ERR wrong number of arguments for 'llen' command\r\n".getBytes());
                            break;
                        }
                        try {
                            key = new String(commandParts.get(1), StandardCharsets.UTF_8);

                            // 调用 DataStore 的核心逻辑
                            int length = DataStore.llen(key);

                            // 将返回的整数格式化为 RESP 响应
                            outputStream.write((":" + length + "\r\n").getBytes());

                        } catch (WrongTypeException e) {
                            outputStream.write(("-"+e.getMessage()+"\r\n").getBytes());
                        }
                        break;
                    case "TYPE":
                        if (commandParts.size() != 2) {
                            outputStream.write("-ERR wrong number of arguments for 'type' command\r\n".getBytes());
                            break;
                        }
                        key = new String(commandParts.get(1), StandardCharsets.UTF_8);

                        // 调用 DataStore 的核心逻辑
                        String type = DataStore.getType(key);

                        // 将返回的类型字符串格式化为 RESP Simple String
                        outputStream.write(("+" + type + "\r\n").getBytes());
                        break;
                    case "XADD":
                        // XADD key id field value [field value ...]
                        if (commandParts.size() < 5 || commandParts.size() % 2 == 0) {
                            outputStream.write("-ERR wrong number of arguments for 'xadd' command\r\n".getBytes());
                            break;
                        }
                        try {
                            key = new String(commandParts.get(1), StandardCharsets.UTF_8);
                            String idStr = new String(commandParts.get(2), StandardCharsets.UTF_8);

                            long timestamp;
                            int sequence;

                            // **关键修复**：必须先判断ID格式，再进行解析
                            if (idStr.endsWith("-*")) {
                                // 这是自动生成序列号的情况
                                timestamp = Long.parseLong(idStr.substring(0, idStr.length() - 2));
                                sequence = -1; // -1 是我们传递给 DataStore 用于自动生成的标志
                            } else {
                                // 这是用户提供了完整 ID 的情况
                                String[] idParts = idStr.split("-");
                                timestamp = Long.parseLong(idParts[0]);
                                sequence = Integer.parseInt(idParts[1]);
                            }

                            // 解析所有键值对 (这部分逻辑是正确的，保持不变)
                            Map<String, byte[]> fields = new HashMap<>();
                            for (int i = 3; i < commandParts.size(); i += 2) {
                                String fieldKey = new String(commandParts.get(i), StandardCharsets.UTF_8);
                                byte[] fieldValue = commandParts.get(i + 1);
                                fields.put(fieldKey, fieldValue);
                            }

                            // 调用 DataStore 的核心逻辑
                            StreamEntryID resultId = DataStore.xadd(key, timestamp, sequence, fields);

                            // 返回 Bulk String 格式的 ID
                            String resultIdStr = resultId.toString();
                            outputStream.write(('$' + String.valueOf(resultIdStr.length()) + "\r\n").getBytes());
                            outputStream.write(resultIdStr.getBytes());
                            outputStream.write("\r\n".getBytes());
                        } catch (NumberFormatException e) {
                            outputStream.write("-ERR Invalid stream ID specified as XADD argument\r\n".getBytes());
                        } catch (Exception e) {
                            // 捕获 WrongTypeException 和 ID 错误
                            outputStream.write(("-ERR "+e.getMessage()+"\r\n").getBytes());
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
