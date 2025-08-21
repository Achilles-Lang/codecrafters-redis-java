package Storage;

import java.util.List;

/**
 * @author Achilles
 * 返回结果的包装类
 */
public class CommandResult {
    public final List<byte[]> parts;
    public final int bytesRead;

    public CommandResult(List<byte[]> parts, int bytesRead) {
        this.parts=parts;
        this.bytesRead=bytesRead;
    }
}
