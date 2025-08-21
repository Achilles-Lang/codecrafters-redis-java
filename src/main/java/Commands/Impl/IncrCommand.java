package Commands.Impl;

import Commands.Command;
import Commands.WriteCommand;
import Service.ClientHandler;
import Storage.DataStore;
import Storage.ValueEntry;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author Achilles
 * @created 09/03/2023 - 11:05
 * Incr 命令: 对字符串类型的键进行自增操作
 * 参数：key
 */
public class IncrCommand implements WriteCommand {
    @Override
    public Object execute(List<byte[]> args, ClientHandler clientHandler) {
        if (args.size() != 1) {
            return new Exception("wrong number of arguments for 'incr' command");
        }

        String key = new String(args.get(0), StandardCharsets.UTF_8);
        DataStore dataStore = DataStore.getInstance();

        // 尝试从 DataStore 获取值
        ValueEntry entry = dataStore.getString(key);

        if (entry == null) {
            // 情况 1: key 不存在，设置为 "1" 并返回 1
            long newValue = 1L;
            dataStore.setString(key, new ValueEntry(String.valueOf(newValue).getBytes(), -1));
            return newValue;
        } else {
            // 情况 2: key 存在
            try {
                // 尝试将值解析为 long
                long currentValue = Long.parseLong(new String(entry.value, StandardCharsets.UTF_8));
                long newValue = currentValue + 1;

                // 将新值存回 DataStore，并返回
                dataStore.setString(key, new ValueEntry(String.valueOf(newValue).getBytes(), -1));
                return newValue;

            } catch (NumberFormatException e) {
                // 如果值不是一个有效的整数，则抛出错误
                return new Exception("value is not an integer or out of range");
            }
        }
    }
}
