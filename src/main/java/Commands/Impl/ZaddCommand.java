// 文件路径: src/main/java/Commands/Impl/ZaddCommand.java

package Commands.Impl;

import Commands.Command;
import Commands.CommandContext;
import Commands.WriteCommand;
import Storage.DataStore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ZaddCommand implements WriteCommand {
    @Override
    public Object execute(List<byte[]> args, CommandContext context) {
        if (args.size() < 3 || args.size() % 2 != 1) {
            return new Exception("wrong number of arguments for 'zadd' command");
        }

        try {
            String key = new String(args.get(0), StandardCharsets.UTF_8);

            // 将参数列表转换为 Object 列表，以便传递给 DataStore
            List<Object> scoresAndMembers = new ArrayList<>();
            for (int i = 1; i < args.size(); i++) {
                scoresAndMembers.add(args.get(i));
            }

            DataStore dataStore = DataStore.getInstance();
            int addedCount = dataStore.zadd(key, scoresAndMembers);

            // ZADD 命令返回新添加的成员数量
            return (long) addedCount;

        } catch (NumberFormatException e) {
            return new Exception("value is not a valid float");
        } catch (Exception e) {
            return e;
        }
    }
}
