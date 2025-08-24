package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;

import java.io.OutputStream;
import java.util.List;

public class MultiCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        return null;
    }
}
