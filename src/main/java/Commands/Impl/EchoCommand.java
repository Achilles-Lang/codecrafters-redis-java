package Commands.Impl;

import Commands.Command;
import Service.ClientHandler;

import java.io.OutputStream;
import java.util.List;

public class EchoCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, ClientHandler clientHandler) {
        if (args.size() != 1) {
            return new Exception("wrong number of arguments for 'echo' command");
        }
        return args.get(0);
    }
}
