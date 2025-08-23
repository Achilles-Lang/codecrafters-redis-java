import Commands.CommandHandler;
import Service.ClientHandler;
import Service.MasterConnectionHandler;
import Storage.DataStore;
import Storage.ReplicationInfo;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
      System.out.println("Logs from your program will appear here!");

      //1.设置默认端口号
      int port = 6379;
      String masterHost = null;
      int masterPort = -1;
      String dir=null;
      String dbFileName=null;

      //2.检查命令行参数
      if (args.length>1){
          for (int i = 0; i < args.length; i++) {
              if ("--port".equalsIgnoreCase(args[i])) {
                  if(i+1<args.length){
                    port=Integer.parseInt(args[i+1]);
                    i++;
                  }
              } else if ("--replicaof".equalsIgnoreCase(args[i])) {
                  if(i+1<args.length){
                      String[] replicaInfo=args[i+1].split("\\s+");
                      if(replicaInfo.length==2){
                          masterHost=replicaInfo[0];
                          masterPort=Integer.parseInt(replicaInfo[1]);
                      }
                      i++;
                  }
              } else if ("--dir".equalsIgnoreCase(args[i])) {
                  if(i+1<args.length){
                      dir=args[i+1];
                      i++;
                  }
              } else if ("--dbfilename".equalsIgnoreCase(args[i])) {
                  if(i+1<args.length){
                      dbFileName=args[i+1];
                      i++;
                  }
              }
          }
          DataStore dataStore=DataStore.getInstance();
          if(dir!=null&&dbFileName!=null){
              dataStore.setRdbConfig(dir, dbFileName);
          }
      }
      CommandHandler commandHandler=new CommandHandler();
      if (masterHost!=null&&masterPort!=-1){
          DataStore.getInstance().setAsReplica(masterHost, masterPort);
          MasterConnectionHandler masterConnectionHandler=new MasterConnectionHandler(masterHost, masterPort,port,commandHandler);
          new Thread(masterConnectionHandler).start();

      }
      try (ServerSocket serverSocket = new ServerSocket(port)) {
          serverSocket.setReuseAddress(true);
          while (true) {
              Socket clientSocket = serverSocket.accept();
              new Thread(new ClientHandler(clientSocket, commandHandler)).start();
          }
      } catch (IOException e) {
          System.out.println("IOException in Main: " + e.getMessage());
      }
  }
}
