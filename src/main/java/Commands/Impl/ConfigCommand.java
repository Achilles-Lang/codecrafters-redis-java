package Commands.Impl;

import Commands.Command;
import Storage.DataStore;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Achilles
 * 实现了 CONFIG 命令
 */
public class ConfigCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, OutputStream os) {
        System.out.println("[DIAGNOSTIC] >>> ConfigCommand.execute() was called! <<<");


        if(args.size() < 2){
            return new Exception("wrong number of arguments for 'config' command");
        }

        String subCommand = new String(args.get(0), StandardCharsets.UTF_8).toLowerCase();

        if("get".equals(subCommand)){
            return handleGet(args.subList(1, args.size()));
        } else{
            return new Exception("Unsupported CONFIG subcommand: " + subCommand);
        }
    }

    private Object handleGet(List<byte[]> getArgs){
        if(getArgs.size()!=1){
            return new Exception("wrong number of arguments for 'config get' command");
        }

        String parameter = new String(getArgs.get(0), StandardCharsets.UTF_8).toLowerCase();
        DataStore dataStore = DataStore.getInstance();

        String value;
        switch(parameter){
            case "dir":
                value = dataStore.getRdbDir();
                break;
            case "dbfilename":
                value = dataStore.getRdbFileName();
                break;
            default:
                return new ArrayList<byte[]>();
        }
        if(value==null){
            return new ArrayList<byte[]>();
        }

        List<byte[]> response=new ArrayList<>();
        response.add(parameter.getBytes(StandardCharsets.UTF_8));
        response.add(value.getBytes(StandardCharsets.UTF_8));

        return response;
    }

}
