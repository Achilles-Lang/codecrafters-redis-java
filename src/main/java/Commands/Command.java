package Commands;

import java.util.List;

/**
 * @author Achilles
 * 所有命令的统一接口
 */
public interface Command {
    Object execute(List<byte[]> args);
}
