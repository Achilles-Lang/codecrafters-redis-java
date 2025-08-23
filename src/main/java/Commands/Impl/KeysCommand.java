package Commands.Impl;

import Commands.Command;
import Storage.DataStore;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Achilles
 * 实现了 KEYS 命令
 */
public class KeysCommand implements Command {
    @Override
    public Object execute(List<byte[]> args, OutputStream os) {
        if(args.size()!=1){
            return new Exception("wrong number of arguments for 'keys' command");
        }

        String pattern = new String(args.get(0), StandardCharsets.UTF_8);

        if("*".equalsIgnoreCase( pattern)){
            DataStore dataStore = DataStore.getInstance();
            List<String> allKeys=dataStore.getAllKeys();
            return allKeys.stream()
                    .map(key->key.getBytes(StandardCharsets.UTF_8))
                    .collect(Collectors.toList());
        } else {
            return List.of();
        }
    }
}
