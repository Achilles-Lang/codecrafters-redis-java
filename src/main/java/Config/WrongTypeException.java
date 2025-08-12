package Config;

/**
 * @author Achilles
 */ // 确保你的项目中有一个 WrongTypeException 类
public class WrongTypeException extends Exception {
    public WrongTypeException(String message) {
        super(message);
    }
}
