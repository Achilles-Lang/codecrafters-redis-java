import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 6379;
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            //1.在循环外部接受一次连接
            clientSocket = serverSocket.accept();
            //获取该连接的输入和输出流
            OutputStream outputStream = clientSocket.getOutputStream();
            InputStream inputStream = clientSocket.getInputStream();

            //创建一个缓冲区来读取数据
            byte[] buffer = new byte[1024];
            //2.循环读取来自同一个客户端的数据
            while(inputStream.read(buffer) != -1){
                // 3.每当读取到数据，就发送一个 PONG 响应
                outputStream.write("+PONG\r\n".getBytes());
                outputStream.flush();
            }
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        } finally {
          try {
            if (clientSocket != null) {
              clientSocket.close();
            }
          } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
          }
        }
  }
}
