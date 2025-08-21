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
                if(commandParts == null || commandParts.isEmpty()){
                    break;
                }

                String commandName=new String(commandParts.get(0), StandardCharsets.UTF_8).toLowerCase();

                if(inTransaction){
                    //如果在事务中
                    if("exec".equals(commandName)){
                        List<Object> results = new LinkedList<>();
                        for (List<byte[]> queuedCommandParts : transactionQueue) {
                            String queuedCommandName = new String(queuedCommandParts.get(0), StandardCharsets.UTF_8);
                            List<byte[]> queuedArgs = queuedCommandParts.subList(1, queuedCommandParts.size());

                            Command commandToExecute = commandHandler.getCommand(queuedCommandName);

                            if(commandToExecute!=null){
                                results.add(commandToExecute.execute(queuedArgs,this));

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
                        Object result=(command==null)
                                ? new Exception("unknown command '" + commandName + "'")
                                :command.execute(args,this);

                        if(command instanceof WriteCommand){
                            List<OutputStream> replicas= DataStore.getInstance().getReplicas();
                            for(OutputStream replica:replicas){
                                try {
                                    RespEncoder.encode(replica,commandParts);
                                    replica.flush();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }

                        if(result instanceof FullResyncResponse){
                            FullResyncResponse resync=(FullResyncResponse) result;

                            String fullResyncLine="+FULLRESYNC " + resync.getMasterReplid() + " " + resync.getMasterReplOffset()+ "\r\n";
                            outputStream.write(fullResyncLine.getBytes(StandardCharsets.UTF_8));

                            byte[] rdbFile= RdbUtil.getEmptyRdbFile();
                            outputStream.write(("$" + rdbFile.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                            outputStream.write(rdbFile);
                        }else {
                            RespEncoder.encode(outputStream, result);

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
}

