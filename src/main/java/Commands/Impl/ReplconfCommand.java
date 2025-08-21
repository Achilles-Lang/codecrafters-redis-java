package Commands.Impl;

import Commands.Command;
import Service.ClientHandler;

import java.io.OutputStream;
import java.util.List;

/**
 * @author Achilles
 * @create 2018/11/01
 * @description 配置复制
 */
public class ReplconfCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, ClientHandler clientHandler) {
        return "OK";
    }
}
