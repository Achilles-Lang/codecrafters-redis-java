package Commands;

import java.io.OutputStream;
import java.util.List;

/**
 * @author Achilles
 * 所有命令的统一接口
 */
public interface Command {
    /**
     * **新增**: 一个特殊的静态对象，用作信号。
     * 当命令返回这个对象时，表示它已经自己处理了响应，
     * 并且客户端的连接状态需要转换到“订阅模式”。
     */
    Object STATE_CHANGE_SUBSCRIBE = new Object();

    Object execute(List<byte[]> args, OutputStream os);
}
