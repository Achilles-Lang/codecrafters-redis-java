import Commands.CommandHandler;
import Service.ClientHandler;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
    System.out.println("Logs from your program will appear here!");

        ServerSocket serverSocket = null;
        int port = 6379;
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            CommandHandler commandHandler=new CommandHandler();

            //主线程等待新连接
            while(true){
                //等待下一个客户端连接，这里会一直阻塞直到有客户端连接进来
                Socket clientSocket=serverSocket.accept();
                System.out.println("Accepted new connection from " + clientSocket.getRemoteSocketAddress());

                //为进来的客户端创建一个线程来处理PING
                Thread clientThread = new Thread(new ClientHandler(clientSocket, commandHandler));

                //启动线程，让线程来响应PING
                clientThread.start();
            }
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        } finally {
          try {
            if (serverSocket != null) {
              serverSocket.close();
            }
          } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
          }
        }
  }
}
