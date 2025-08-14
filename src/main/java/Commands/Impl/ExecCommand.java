package Commands.Impl;

import Commands.Command;

import java.util.List;

public class ExecCommand implements Command {
    @Override
    public Object execute(List<byte[]> args) {
        return new Exception("EXEC without MULTI");
    }
}
