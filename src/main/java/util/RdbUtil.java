package util;

import java.util.Base64;

/**
 * @author Achilles
 */
public class RdbUtil {
    // 这是世界上最小的合法 RDB 文件的 Base64 编码
    //一个合法的、空的 RDB 文件不是 0 字节，而是一段特定的二进制内容。
    // 你可以直接使用下面这个已经 Base64 编码好的空 RDB 文件内容。
    private static final String EMPTY_RDB_BASE64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/woGeoZGorZmG1";

    public static byte[] getEmptyRdbFile() {
        return Base64.getDecoder().decode(EMPTY_RDB_BASE64);
    }}
