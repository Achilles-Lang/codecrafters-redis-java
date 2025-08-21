package Commands.Impl;

import Commands.Command;

import java.io.OutputStream;
import java.util.List;

public class ExecCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, OutputStream os) {
        return new Exception("EXEC without MULTI");
    }
}
