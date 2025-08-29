package Commands;

import Commands.Impl.*;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Achilles
 * 命令的注册与分支
 *
 */
public class CommandHandler {
    private final Map<String, Command> commandMap=new HashMap<>();

    public CommandHandler()
    {
        commandMap.put("ping",new PingCommand());
        commandMap.put("echo",new EchoCommand());
        commandMap.put("set",new SetCommand());
        commandMap.put("get",new GetCommand());
        commandMap.put("type",new TypeCommand());
        commandMap.put("rpush",new RpushCommand());
        commandMap.put("lpush",new LpushCommand());
        commandMap.put("lpop",new LpopCommand());
        commandMap.put("llen",new LlenCommand());
        commandMap.put("lrange",new LrangeCommand());
        commandMap.put("xadd",new XaddCommand());
        commandMap.put("xrange",new XrangeCommand());
        commandMap.put("blpop",new BlpopCommand());
        commandMap.put("xread",new XreadCommand());
        commandMap.put("incr",new IncrCommand());
        commandMap.put("exec",new ExecCommand());
        commandMap.put("multi",new MultiCommand());
        commandMap.put("info",new InfoCommand());
        commandMap.put("replconf",new ReplconfCommand());
        commandMap.put("psync",new PsyncCommand());
        commandMap.put("wait",new WaitCommand());
        commandMap.put("config",new ConfigCommand());
        commandMap.put("keys",new KeysCommand());
        commandMap.put("subscribe",new SubscribeCommand());
        commandMap.put("publish",new PublishCommand());
        commandMap.put("unsubscribe",new UnsubscribeCommand());
        commandMap.put("zadd",new ZaddCommand());
        commandMap.put("zrank",new ZrankCommand());
        commandMap.put("zrange",new ZrangeCommand());
        commandMap.put("zcard",new ZcardCommand());
    }
    public Command getCommand(String commandName)
    {
        return commandMap.get(commandName.toLowerCase());
    }
}
