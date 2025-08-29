// 文件路径: src/main/java/Commands/Command.java
package Commands;

import java.util.List;

public interface Command {

    // ===> 新增内容开始 <===
    /**
     * 一个特殊的静态对象，代表 RESP 的 Null Bulk String ("$-1\r\n")。
     * 用于 GET 等命令找不到 key 时的返回值。
     */
    Object NULL_BULK_STRING_RESPONSE = new Object();

    /**
     * 一个特殊的静态对象，代表 RESP 的 Null Array ("*-1\r\n")。
     * 用于 XREAD 等命令超时时的返回值。
     */
    Object NULL_ARRAY_RESPONSE = new Object();
    // ===> 新增内容结束 <===


    /**
     * 一个特殊的静态对象，用作信号。
     * 当命令返回这个对象时，表示它已经自己处理了响应，
     * 并且客户端的连接状态需要转换到“订阅模式”。
     */
    Object STATE_CHANGE_SUBSCRIBE = new Object();

    /**
     * 执行命令的核心方法。
     * @param args 命令的参数
     * @param context 包含客户端状态和输出流的上下文
     * @return 命令执行的结果
     */
    Object execute(List<byte[]> args, CommandContext context);
}
