package Commands.Impl;

import Commands.Command;

import java.io.OutputStream;
import java.util.List;

public class MultiCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, OutputStream os) {
        return null;
    }
}
