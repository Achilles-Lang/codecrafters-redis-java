// 文件路径: src/main/java/Commands/CommandContext.java

package Commands;

import java.io.OutputStream;

/**
 * @author Achilles
 * 一个上下文对象，封装了执行命令时可能需要的所有客户端特定信息。
 * 这使得命令类本身可以保持无状态。
 */
public class CommandContext {
    private final OutputStream outputStream;
    private boolean isClientSubscribed = false;

    // 唯一的、清晰的构造函数
    public CommandContext(OutputStream os, boolean isClientSubsigned) {
        this.outputStream = os;
        this.isClientSubscribed = isClientSubsigned;
    }

    // 一个更简单的构造函数，用于不需要订阅状态的场景
    public CommandContext(OutputStream os) {
        this(os, false);
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    public boolean isClientSubscribed() {
        return isClientSubscribed;
    }

    public void enterSubscribeMode() {
        this.isClientSubscribed = true;
    }

    public void exitSubscribeMode() {
        this.isClientSubscribed = false;
    }
}
