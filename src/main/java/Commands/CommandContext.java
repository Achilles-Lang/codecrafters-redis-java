// 文件路径: src/main/java/Commands/CommandContext.java

package Commands;

import Service.Protocol; // <--- 导入 Protocol 类

import java.io.OutputStream;

public class CommandContext {
    private final OutputStream outputStream;
    private final Protocol parser; // <--- 新增字段，用于携带 Protocol 解析器

    // ===> 为普通客户端保留的构造函数 <===
    public CommandContext(OutputStream outputStream) {
        this.outputStream = outputStream;
        this.parser = null; // 普通客户端没有这个
    }

    // ===> 为 MasterConnectionHandler 设计的、新的构造函数 <===
    public CommandContext(OutputStream outputStream, Protocol parser) {
        this.outputStream = outputStream;
        this.parser = parser;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    // ===> 新增的 getter 方法 <===
    public Protocol getParser() {
        return parser;
    }
}
