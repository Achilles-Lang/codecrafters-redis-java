# Introduction
顺着往下，选择熟悉的语言，选择对语言的熟悉程度，选择是否允许发邮件给你。
# Repository Setup
- 首先，CodeCrafters提供的指令是为**命令行**设计的，也就是说GitHub Desktop中输入这些命令是行不通的。
- 因此，你必须确保你的电脑上有一个命令行终端：
   - 在Windows上，推荐使用Git Bash，Command Prompt或者PowerShell，通俗的来说，win+r后输入cmd回车。
   - 在macOS或者Linux上，使用Terminal。
- 然后，你需要确保电脑上安装了Git。可以在终端中输入`git --version` 来检查。
## step 1:克隆仓库(Clone the repository)
1. 打开你的终端。
2. 导航到你希望存放项目的文件夹。在windows中，在上方文件目录栏处输入cmd即可，当然，终端中使用cd也可以。
   ``` bash
   # 对于 macOS/Linux 
   cd ~/Documents/Projects 
   # 对于 Windows 
   cd C:\Users\YourUsername\Documents\Projects
   ```
3. **复制粘贴CodeCrafters 提供的第一条命令**到你的终端，然后按回车。
   ``` bash
   git clone https://git.codecrafters.io/xxxx
   ```
4. 复制第二条命令
   ``` bash
   cd codecrafters-redis-java
   ```
## step 2:推送一个空提交(Push an empty commit)
这一步的目的是为了验证你的本地环境和 CodeCrafters 服务器之间的通信是正常的。
1. **创建一个空的提交**。复制指令，然后回车。
   ``` bash
   git commit --allow-empty -m 'test'
   ```
2. **将这个提交推送到 CodeCrafters 的服务器**。
   ``` bash
   git push origin master
   ```
   这条命令会将你刚刚在本地创建的提交上传到 CodeCrafters 的远程仓库 (`origin`) 的 `master` 分支上。
   然后返回到 CodeCrafters 的网页，你应该会看到页面上的 "Listening for a git push" 消息发生变化，并且第一个阶段会被标记为通过

# Bind to a port
由于是第一阶段，因此任务不是很难，只需要取消注释代码并提交更改即可。
##  Step 1:Navigate to src/main/java/Main.java
- 用IDEA工具打开项目，并根据目录找到Main.java
- 如果使用的是 VS Code，运行指令：
  ``` bash
  code --goto src/main/java/Main.java .
  ```
## Step 2:Uncomment code
- 打开Main.java，然后将其中和代码相关的注释删除掉，保存即可。
- 是不是代码大家都应该看的出来。
## Step 3:Submit changes
- 这里将会使用到git相关的指令，不过都很简单。
- 首先，在项目文件下的终端中输入以下指令：
  ``` bash
  git commit -am "[any message]"
  ```
  在git中，git commit命令用于记录你的更改，-am由-a和-m组成。-a用于提交所有更改的快照，即提交已添加的文件（Main.java），同时不会提交新创建的文件。-m选项允许你在命令行上编写提交信息，也就是中括号中的部分。你可以这样：git commit -am "fix the Main.java"。
- 等出现提示之后，再运行指令：
  ``` bash
  git push origin master
  ```
  这个指令用于将本地更改推送到远程存储库，其中，`origin`是远程存储卡的名称，通常默认为远程存储库的 URL 或者其他别名。`master` 是本地分支的名称，代表您希望推送的本地更改所在的分支。
- 如果失败了看看是不是端口被占用。
# Respond to PING
98%的用户都能通过。
在此阶段，你将实现对 PING 命令的支持。
## **Step 1**: Implement solution
- 我们需要将PONG响应发送到客户端Socket的OutputStream。
- 把代码复制了粘贴到对应位置即可。
- 位置在Main.java中。
- 然后再添加一行代码：
  ``` java
  //在发送响应后 强制刷新缓冲区并保持连接开启
  outputStream.flush();
  ```
- 这两行代码的作用是：通过Socket连接向客户端发送一个简单的回复消息 "+PONG"
## **Step 2**: Run tests
- git指令，照做就行。
# Respond to multiple PINGs
上一阶段响应了一个PING，这阶段我们将响应多个PING。
## **Test**
思路：
- 首先，测试工具只会连接一次你的服务器，在同一个连接上，会持续发送多个PING命令，它希望你在同一个连接上，每发送一个PING，就受到一个PONG作为回应。我们需要做的是，如果读取到一个数据，就返回一个PANG。
- 这就需要使用循环了，循环条件显而易见：有没有收到数据，如果有，返回PANG。
- 那么数据从哪里来？连接后的输入流中可以检查是否含有数据，inputStream.read()可以用来完成检查。
- 可是，PING命令如果是`*1\r\n$4\r\nPING\r\n`，那按照`while(inputStream.read() != -1)`，一个PING就会返回14次PONG。显然，不符合预期。
- 解决方法就是，使用`while(inputStream.read(buffer) != -1)` ，因为测试程序发送一个完整的命令后就会停下来等待你的响应，此时，你的 `inputStream.read(buffer)` 会一次性把刚才那个 `PING` 命令的所有字节都读到缓冲区里。然后循环，发送一个PONG。
  代码：
``` java
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
```
# Handle concurrent clients
这一阶段，需要完成对多个并发客户端的支持。
也就是说，要处理来此多个客户端的PING。
如果要实现此功能，需要使用线程。
## Tests
思路：
- 上一个阶段，我们能够应付一个连接的多次PING，这一次需要处理多个连接，还好，提示说可以使用线程(Thread)。
- 问题是如何接受多个客户端呢？答案是：主线程在一个无限循环中不停地调用。
- 主线程只需要做一件事：无限循环，等待新的客户端连接。类似接待员，只站在门口等新客人。
- 等新的客户端到来，我们将创建一个新线程，让新线程为这个客户端服务。类似于接待员让一个服务员领着客人去潇洒。
- 而接待员将客人交给服务员后，继续在门口等待下一位客人。
- 而服务员需要干的事，就是上一阶段的任务。
- 基于上面的猜测，我们需要对代码进行修改：
   - 创建一个`Service.ClientHandler`类，这个类实现了Runnable接口，作用是”服务员“。
   - 修改main方法，让他当好接待员。
     代码：
- Service.ClientHandler.java
``` java
import java.io.IOException;  
import java.io.InputStream;  
import java.io.OutputStream;  
import java.net.ServerSocket;  
import java.net.Socket;  
  
/**  
 * @author Achilles  
 */public class Service.ClientHandler implements Runnable{  
  
    private Socket clientSocket;  
  
    public Service.ClientHandler(Socket socket) {  
        this.clientSocket = socket;  
    }  
  
    public Socket getClientSocket() {  
        return clientSocket;  
    }  
  
    @Override  
    public void run( ) {  
        //获取该连接的输入和输出流  
        OutputStream outputStream = null;  
        try (Socket socket=this.clientSocket) {  
            System.out.println("New client handler thread started for " + socket.getRemoteSocketAddress());  
  
            InputStream inputStream = clientSocket.getInputStream();  
            outputStream = clientSocket.getOutputStream();  
            byte[] buffer = new byte[1024];  
            while(inputStream.read(buffer) != -1){  
                // 每当读取到数据，就发送一个 PONG 响应  
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
```
- Main.java
``` java
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
        int port = 6379;  
        try {  
            serverSocket = new ServerSocket(port);  
            serverSocket.setReuseAddress(true);  
  
            //主线程等待新连接  
            while(true){  
                //等待下一个客户端连接，这里会一直阻塞直到有客户端连接进来  
                Socket clientSocket=serverSocket.accept();  
                System.out.println("Accepted new connection from " + clientSocket.getRemoteSocketAddress());  
  
                //为进来的客户端创建一个线程来处理PING  
                Thread clientThread = new Thread(new Service.ClientHandler(clientSocket));  
  
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
```
# Implement the ECHO command
现在，我们将从PINGPONG来到ECHO命令。
ECHO 是一个类似 PING 的命令，用于测试和调试。
它接受一个参数，并将其作为 RESP 批量字符串返回。
例如：
``` bash
redis 127.0.0.1:6379> ECHO "Hello World"
"Hello World"
```
## Tests
在此之前，我们需要先写一个Redis协议解析器，后续阶段会用到。
### Redis协议解析器
1. 为什么要有 Redis 协议解析器？(The "Why")
   - 简单来说，因为网络传输的本质是**无差别、无结构的字节流（Byte Stream）**。
   - 而一个真正的Redis服务器必须能看懂命令，在之前的阶段，我们能够回复`+PONG\r\n`，是因为我们根本不管测试内容，只要吱一声我们就默认是PING。
   - 而当客户端发送一个命令时，它不会直接以这个字符串的形式发送。它会遵循一种叫做 **RESP (REdis Serialization Service.Protocol)** 的格式，类似：`*4\r\n$3\r\nSET\r\n$7\r\nuser:1\r\n$4\r\nname\r\n$5\r\nAlice\r\n`。
   - 而Redis协议解析器的根本目的就是看懂这串天书，即：将无结构、无意义的字节流，翻译成程序可以理解和执行的、有结构的数据（比如，一个包含命令和参数的列表 `["SET", "user:1", "name", "Alice"]`）。
   - 是的，就是一个翻译。
2. 如何实现？(The "How")
   - 实现一个协议解析器的经典方法是**状态机**（State Machine）模型。
   - 思路如下：
      1. **维护一个内部缓冲区（Internal Buffer）**: 创建一个 `byte[]` 或使用 `ByteBuffer` 来存放从 Socket 读入但尚未完全解析的数据。
      2. **定义解析状态**: 用一个枚举（Enum）或变量来表示解析器当前所处的状态。
      3. **循环解析**:
         - 从 Socket 读取新数据，追加到内部缓冲区。
         - 进入一个循环，尝试从缓冲区中解析出一个完整的 RESP 数据对象。
         - **在循环中，根据当前状态执行操作**：
            - **检查缓冲区数据是否足够**：例如，如果状态是 `READING_SIMPLE_STRING`，就检查缓冲区中是否有 `\r\n`。如果没有，就退出解析循环，等待下一次从 Socket 读取更多数据。
            - **解析数据**：如果数据足够，就根据规则解析出一个值（比如一个字符串 "OK" 或者一个整数 123）。
            - **消耗缓冲区**：将已解析的字节从缓冲区中移除（或者移动缓冲区的读取指针）。
            - **更新状态**：根据解析出的内容更新状态。例如，如果解析完一个数组的长度 `*3`，下一步的状态就应该是去解析数组的第一个元素。
            - **重复**：如果缓冲区中还有剩余数据（处理“粘包”情况），就继续在循环内解析下一个对象。
### 代码实现：
- 官方在评论区放了可以参考的开源项目，这这边放一下使用Java实现的Jedis的Redis协议解析器链接：https://github.com/redis/jedis/blob/11a4513ff9581a40530a84e6c8ee019c4a3f9e38/src/main/java/redis/clients/jedis/Protocol.java
- 简化简单版本：
``` java
import java.io.ByteArrayOutputStream;  
import java.io.IOException;  
import java.io.InputStream;  
import java.nio.charset.StandardCharsets;  
import java.util.ArrayList;  
import java.util.List;  
  
public class Service.Protocol {  
    private final InputStream inputStream;  
  
    public Service.Protocol(InputStream inputStream) {  
        this.inputStream = inputStream;  
    }  
    /*  
    * 读取并解析一个完整的Redis命令  
    * 在这个版本中，我们假设所有命令都以数组形式开始  
    * @return 解析后的命令及参数列表，以字节数组列表形式返回。如果流关闭则返回 null。  
    * */    public List<byte[]> readCommand() throws IOException {  
        int firstByte = inputStream.read();  
        if (firstByte == -1) {  
            //连接已关闭  
            return null;  
        }  
  
        char type = (char) firstByte;  
        if (type == '*') {  
            return parseArray();  
        } else {  
            throw new IOException("Unsupported command format. Expected an Array ('*').");  
        }  
    }  
  
    private List<byte[]> parseArray() throws IOException {  
        //读取数组长度  
        int arraySize = Integer.parseInt(readLine());  
        if (arraySize <= 0) {  
            return new ArrayList<>();  
        }  
        //循环读取数组中的每一个元素  
        List<byte[]> commandParts = new ArrayList<>(arraySize);  
        for (int i = 0; i < arraySize; i++) {  
            int typeByte = inputStream.read();  
            char elementType = (char) typeByte;  
            if (elementType == '$') {  
                commandParts.add(parseBulkString());  
            } else {  
                throw new IOException("Unsupported element type in Array. Expected Bulk String ('$').");  
            }  
        }  
        return commandParts;  
    }  
  
    private byte[] parseBulkString() throws IOException {  
        //读取字符串长度  
        int stringLength = Integer.parseInt(readLine());  
        if (stringLength == -1) {  
            return null;  
        }  
  
        //读取指定长度的字节数据  
        byte[] data = new byte[stringLength];  
        int bytesRead = 0;  
        while (bytesRead < stringLength) {  
            int read = inputStream.read(data, bytesRead, stringLength - bytesRead);  
            if (read == -1) {  
                throw new IOException("Unexpected end of stream while reading Bulk String data.");  
            }  
            bytesRead += read;  
        }  
  
        if (inputStream.read() != '\r' || inputStream.read() != '\n') {  
            throw new IOException("Expected CRLF after Bulk String data.");  
        }  
  
        return data;  
    }  
    /*  
    * 从输入流中读取一行，以\r\n结尾  
    * @return 读取到的行内容 (不包含 \r\n)    * */    private String readLine() throws IOException {  
        ByteArrayOutputStream baos = new ByteArrayOutputStream();  
        int b;  
        while ((b = inputStream.read()) != '\r') {  
            if (b == -1) {  
                throw new IOException("Unexpected end of stream.");  
            }  
            baos.write(b);  
        }  
        if (inputStream.read() != '\n') {  
            throw new IOException("Expected LF after CR.");  
        }  
        return baos.toString(StandardCharsets.UTF_8);  
    }  
}
```
### tests
接下来，运用简单版本的协议，来对输入流进行解析。
- Main.java：不用修改，继续当接待员。
- Service.ClientHandler.java：处理事务，需要修改，使用Protocol来处理命令。
   - 对输入数据流，要判断是不是指令，是什么指令，该指令要我们干什么。
``` java
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
 */public class Service.ClientHandler implements Runnable{  
  
    private final Socket clientSocket;  
  
    public Service.ClientHandler(Socket socket) {  
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
            Service.Protocol protocol=new Service.Protocol(socket.getInputStream());  
  
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
```

# Implement the SET & GET Commands
上一阶段完成了ECHO命令，现在到了GET和SET命令了。
## Tests
- 数据和逻辑需要分离，因此Protocol只管解析，再设计一个类DataStore来进行存取，而ClientHandler只管协调。
- 使用HashMap完成GET和SET。
- code：
- Storage.DataStore.java
``` java
import java.util.concurrent.ConcurrentHashMap;  
import java.util.Map;  
  
/**  
 * @author Achilles  
 */public class Storage.DataStore {  
    private static final Map<String, byte[]> map = new ConcurrentHashMap<>();  
  
    public static void set(String key, byte[] value) {  
        map.put(key, value);  
    }  
  
    public static byte[] get(String key) {  
        return map.get(key);  
    }  
}
```
- Service.ClientHandler.java
``` java
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
 */public class Service.ClientHandler implements Runnable{  
  
    private final Socket clientSocket;  
  
    public Service.ClientHandler(Socket socket) {  
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
            Service.Protocol protocol=new Service.Protocol(socket.getInputStream());  
  
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
                            Storage.DataStore.set(key, value);  
                            outputStream.write("+OK\r\n".getBytes());  
                        } else {  
                            outputStream.write("-ERR wrong number of arguments for 'set' command\r\n".getBytes());  
                        }  
                        break;  
                    case "GET":  
                        if (commandParts.size() > 1) {  
                            String key = new String(commandParts.get(1), StandardCharsets.UTF_8);  
                            byte[] value = Storage.DataStore.get(key);  
                            if (value != null) {  
                                // 找到了值，以 Bulk String 格式返回  
                                outputStream.write(('$' + String.valueOf(value.length) + "\r\n").getBytes());  
                                outputStream.write(value);  
                                outputStream.write("\r\n".getBytes());  
                            } else {  
                                // 没找到值，返回 Null Bulk String                                outputStream.write("$-1\r\n".getBytes());  
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
```
# Expiry
- SET命令中可以使用PX参数设置，添加密钥的有效期，有效期以毫秒为单位。
- 密钥过期后，对该密钥的GET命令应该返回空批量字符($-1\r\n）。
- 在这一阶段，我们就需要对SET命令进行完善。
- Redis官方实现以两种方式处理过期：主动和被动。在这个阶段，最简单的实现是被动，也就是无需担心运行单独的线程来主动删除键，只需在收到请求时检查值是否过期即可。
- 那么DataStore就得进行升级了，因为还需要存储每个键的过期时间戳。
## Tests
1. 创建一个新的数据结构来包装值，能同时包含值和过期时间。可以使用内部类或者Java Record来做这件事。
``` java
//Storage.ValueEntry.java
/**
 * 用于封装存储在 Storage.DataStore 中的值及其元数据。
 */
class Storage.ValueEntry {
    final byte[] value;
    final long expiryTimestamp; // 过期的绝对时间点 (毫秒)

    // expiryTimestamp = -1 表示永不过期
    public Storage.ValueEntry(byte[] value, long expiryTimestamp) {
        this.value = value;
        this.expiryTimestamp = expiryTimestamp;
    }

    /**
     * 检查这个条目是否已经过期。
     * @return 如果已过期则返回 true，否则返回 false。
     */
    public boolean isExpired() {
        if (expiryTimestamp == -1) {
            return false; // 永不过期
        }
        return System.currentTimeMillis() > expiryTimestamp;
    }
}
```
2. 升级DataStore
``` java
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

class Storage.DataStore {
    // 将 Map 的值类型从 byte[] 改为 Storage.ValueEntry
    private static final Map<String, Storage.ValueEntry> map = new ConcurrentHashMap<>();

    // SET 方法需要能接收一个可选的过期时间
    public static void set(String key, byte[] value, long ttlMillis) {
        long expiryTimestamp;
        if (ttlMillis > 0) {
            // 计算绝对过期时间点
            expiryTimestamp = System.currentTimeMillis() + ttlMillis;
        } else {
            // -1 或 0 表示永不过期
            expiryTimestamp = -1;
        }
        map.put(key, new Storage.ValueEntry(value, expiryTimestamp));
    }

    // GET 方法中实现“被动删除”的核心逻辑
    public static byte[] get(String key) {
        Storage.ValueEntry entry = map.get(key);

        if (entry == null) {
            return null; // Key 不存在
        }

        // 核心：在访问时检查是否过期
        if (entry.isExpired()) {
            // 如果已过期，就从 map 中删除它，并返回 null
            map.remove(key);
            return null;
        }

        // 如果未过期，正常返回值
        return entry.value;
    }
}
```
3. 修改ClientHandler以解析PX参数
``` java
case "SET":  
    if (commandParts.size() < 3) {  
        outputStream.write("-ERR wrong number of arguments for 'set' command\r\n".getBytes());  
        break;  
    }  
  
    String key = new String(commandParts.get(1), StandardCharsets.UTF_8);  
    byte[] value = commandParts.get(2);  
    long ttl = -1; // 默认永不过期  
  
    // --- 解析可选的 PX 参数 ---    if (commandParts.size() > 3) {  
        for (int i = 3; i < commandParts.size(); i++) {  
            String option = new String(commandParts.get(i), StandardCharsets.UTF_8).toUpperCase();  
            if ("PX".equals(option)) {  
                // PX 的下一个参数是过期时间  
                if (i + 1 < commandParts.size()) {  
                    try {  
                        ttl = Long.parseLong(new String(commandParts.get(i + 1)));  
                        i++; // 跳过下一个参数，因为它已经被消费了  
                    } catch (NumberFormatException e) {  
                        outputStream.write("-ERR value is not an integer or out of range\r\n".getBytes());  
                        // 可以选择直接返回，避免执行 set                        return;  
                    }  
                } else {  
                    outputStream.write("-ERR syntax error\r\n".getBytes());  
                    return;  
                }  
            }  
        }  
    }  
  
    Storage.DataStore.set(key, value, ttl);  
    outputStream.write("+OK\r\n".getBytes());  
    break;  
case "GET":  
    if (commandParts.size() > 1) {  
        String getKey = new String(commandParts.get(1), StandardCharsets.UTF_8);  
        byte[] getValue = Storage.DataStore.get(getKey);  
        if (getValue != null) {  
            // 找到了值，以 Bulk String 格式返回  
            outputStream.write(('$' + String.valueOf(getValue.length) + "\r\n").getBytes());  
            outputStream.write(getValue);  
            outputStream.write("\r\n".getBytes());  
        } else {  
            // 没找到值，返回 Null Bulk String            outputStream.write("$-1\r\n".getBytes());  
        }  
    } else {  
        outputStream.write("-ERR wrong number of arguments for 'get' command\r\n".getBytes());  
    }  
    break;
```
