package Commands;

import java.io.OutputStream;
import java.util.List;

/**
 * @author Achilles
 * 所有命令的统一接口
 */
public interface Command {
    // 代表 $-1
    Object NULL_BULK_STRING_RESPONSE=new Object();
    // 代表 *-1
    Object NULL_ARRAY_RESPONSE=new Object();

    /**
     * **新增**: 一个特殊的静态对象，用作信号。
     * 当命令返回这个对象时，表示它已经自己处理了响应，
     * 并且客户端的连接状态需要转换到“订阅模式”。
     */
    Object STATE_CHANGE_SUBSCRIBE = new Object();
    /**
     * **关键修改**: execute 方法现在接收一个 CommandContext 对象。
     * @param args 命令的参数
     * @param context 包含客户端状态和输出流的上下文
     * @return 命令执行的结果
     */
    Object execute(List<byte[]> args, CommandContext context);
}
