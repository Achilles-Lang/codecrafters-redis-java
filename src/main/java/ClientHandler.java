import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author Achilles
 */
public class ClientHandler implements Runnable{

    private Socket clientSocket;

    public ClientHandler(Socket socket) {
        this.clientSocket = socket;
    }

    public Socket getClientSocket() {
        return clientSocket;
    }

    @Override
    public void run( ) {
        //1.在循环外部接受一次连接
        //获取该连接的输入和输出流
        OutputStream outputStream = null;
        try (Socket socket=this.clientSocket) {
            System.out.println("New client handler thread started for " + socket.getRemoteSocketAddress());

            InputStream inputStream = clientSocket.getInputStream();
            outputStream = clientSocket.getOutputStream();
            byte[] buffer = new byte[1024];
            while(inputStream.read(buffer) != -1){
                // 3.每当读取到数据，就发送一个 PONG 响应
                outputStream.write("+PONG\r\n".getBytes());
                outputStream.flush();
            }
        } catch (IOException e) {
            System.out.println("Connection closed for " + clientSocket.getRemoteSocketAddress() + ": " + e.getMessage());
        }finally {
            System.out.println("Client handler thread finished for " + clientSocket.getRemoteSocketAddress());

        }

    }


}
