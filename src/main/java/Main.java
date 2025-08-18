import Commands.CommandHandler;
import Service.ClientHandler;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
      System.out.println("Logs from your program will appear here!");

      //1.设置默认端口号
      int port = 6379;

      //2.检查命令行参数，查找“--port”
      if (args.length>1){
          for (int i = 0; i < args.length; i++) {
              if ("--port".equalsIgnoreCase(args[i])) {
                  if(i+1<args.length){
                      try {
                          port = Integer.parseInt(args[i+1]);
                          break;
                      } catch (NumberFormatException e){
                          System.out.println("Invalid port number: " + args[i + 1]);
                          return;
                      }
                  }
              }
          }
      }
      System.out.println("Server is configured to listen on port: " + port);

      try (ServerSocket serverSocket = new ServerSocket(port)) {
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
