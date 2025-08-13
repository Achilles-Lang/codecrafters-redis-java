import Commands.CommandHandler;
import Service.ClientHandler;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
      System.out.println("Logs from your program will appear here!");

      try (ServerSocket serverSocket = new ServerSocket(6379)) {
          serverSocket.setReuseAddress(true);
          CommandHandler commandHandler = new CommandHandler(); // 创建一个命令处理器

          while (true) {
              Socket clientSocket = serverSocket.accept();
              // 将命令处理器传递给每个客户端线程
              Thread clientThread = new Thread(new ClientHandler(clientSocket, commandHandler));
              clientThread.start();
          }
      } catch (IOException e) {
          System.out.println("IOException in Main: " + e.getMessage());
      }
  }
}
