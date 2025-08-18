package Commands.Impl;

import Commands.Command;

import java.util.List;

/**
 * @author Achilles
 */
public class ReplconfCommand implements Command {
    @Override
    public Object execute(List<byte[]> args) {
        return "OK";
    }
}
