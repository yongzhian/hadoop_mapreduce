package cn.zain.mr;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringTools {

    private static final Logger logger = Logger.getLogger(StringTools.class);

    /**
     * 功能说明 : byte数组转base64的字符串
     *
     * @param buf
     * @return
     */
    public static String byte2BASE64(byte[] buf) {
        try {
            return new String(new Base64().encode(buf));
        } catch (Exception e) {
            logger.error("将byte转为base64的字符串失败.", e);
        }
        return null;
    }

    /**
     * 功能说明 : 将二进制转换成16进制
     *
     * @param buf
     * @return
     */
    public static String byte2HexStr(byte buf[]) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < buf.length; i++) {
            String hex = Integer.toHexString(buf[i] & 0xFF);
            if (hex.length() == 1) {
                hex = '0' + hex;
            }
            sb.append(hex);
        }
        return sb.toString();
    }

    /**
     * 功能说明 : 生成指定长度的随机数 纯数字
     *
     * @param len
     * @return
     */
    public static String genRandomNum(int len) {
        StringBuffer result = new StringBuffer();
        for (int i = 0; i < len; i++) {
            result.append((int) (Math.random() * 10));
        }
        return result.toString();
    }

    /**
     * 功能说明 : 生成指定长度的随机字符串
     *
     * @param length
     * @return
     */
    public static String genRandomStr(int length) { // length表示生成字符串的长度
        String base = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }

    /**
     * 功能说明 : 从map中获取key并转为Long
     *
     * @param map
     * @param key
     * @return
     */
    public static long getLong(Map<String, Object> map, String key) {
        if (null == map || map.isEmpty()) {
            return 0L;
        }
        return StringTools.obj2Long(map.get(key));
    }

    /**
     * 功能说明 : 获取当前时间 格式 yyyy-MM-dd HH:mm:ss
     *
     * @return
     */
    public static String getNow() {
        return getNow("yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 功能说明 : 获取当前时间，自定义格式
     *
     * @param format
     * @return
     */
    public static String getNow(String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);

        return sdf.format(new Date());
    }

    /**
     * 功能说明 : 获取当前时间，自定义格式
     *
     * @param format
     * @return
     */
    public static String getFormatDate(String format, Date date) {
        if (null == date) {
            return null;
        }
        if (StringUtils.isBlank(format)) {
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }

    /**
     * 功能说明 : 获取当前日期 格式 yyyy-MM-dd
     *
     * @return
     */
    public static String getNowDate() {
        return getNow("yyyy-MM-dd");
    }

    /**
     * 功能说明 :从map中获取key的值,默认为""
     *
     * @param map
     * @param key
     * @return
     */
    public static String getStr(Map<String, Object> map, String key) {
        return StringTools.getStr(map, key, "");
    }

    /**
     * 功能说明 :从map中获取key的值,默认为defaultStr
     *
     * @param map
     * @param key
     * @param defaultStr
     * @return
     */
    public static String getStr(Map<String, Object> map, String key, String defaultStr) {
        if (null == map || map.isEmpty()) {
            return defaultStr;
        }
        return StringTools.nvl(map.get(key), defaultStr);
    }

    /**
     * 功能说明 : 获取字符串最后4个字符，若空或小于等于4位则直接返回
     *
     * @return
     */
    public static String getLast4Char(String srcStr) {
        if (StringUtils.isBlank(srcStr) || srcStr.length() <= 4) {
            return srcStr;
        }
        return "****" + srcStr.substring(srcStr.length() - 4, srcStr.length());
    }

    /**
     * 功能说明 : 将16进制转换为二进制
     *
     * @param hexStr
     * @return
     */
    public static byte[] hexStr2Byte(String hexStr) {
        if (hexStr.length() < 1)
            return null;
        byte[] result = new byte[hexStr.length() / 2];
        for (int i = 0; i < hexStr.length() / 2; i++) {
            int high = Integer.parseInt(hexStr.substring(i * 2, i * 2 + 1), 16);
            int low = Integer.parseInt(hexStr.substring(i * 2 + 1, i * 2 + 2),
                    16);
            result[i] = (byte) (high * 16 + low);
        }
        return result;
    }

    /**
     * 功能说明 : 判断字符串是否是1-100的数字
     *
     * @param str
     * @return
     */
    public static boolean isOne2HundredNumber(String str) {
        if (null == str || StringUtils.isBlank(str)) {
            return false;
        }
        Pattern pattern = Pattern.compile("[1-9]\\d?|100");
        return pattern.matcher(str).matches();
    }

    /**
     * 功能说明 : 判断字符串是否是0-100的数字
     *
     * @param str
     * @return
     */
    public static boolean isZero2HundredNumber(String str) {
        if (StringUtils.isBlank(str)) {
            return false;
        }
        Pattern pattern = Pattern.compile("[1-9]\\d?|100|0");
        return pattern.matcher(str).matches();
    }


    /**
     * 功能说明 : 判断字符串是否全是数字
     *
     * @param str
     * @return
     */
    public static boolean isNumber(String str) {
        if (null == str || StringUtils.isBlank(str)) {
            return false;
        }
        Pattern pattern = Pattern.compile("[0-9]*");
        return pattern.matcher(str).matches();
    }

    /**
     * 功能说明 : 判断字符串是否是指定长度的数字
     *
     * @param str
     * @return
     */
    public static boolean isNumber(String str, int len) {
        if (StringUtils.isBlank(str)) {
            return false;
        }
        Pattern pattern = Pattern.compile("[0-9]{" + len + "}");
        return pattern.matcher(str).matches();
    }

    /**
     * 功能说明 : 判断字符串是否是指定范围长度的数字
     *
     * @param str
     * @return
     */
    public static boolean isNumber(String str, int minLen, int maxLen) {
        if (StringUtils.isBlank(str)) {
            return false;
        }
        Pattern pattern = Pattern.compile("[0-9]{" + minLen + "," + maxLen + "}");
        return pattern.matcher(str).matches();
    }

    /**
     * 判断是否是小数（整数也认为是小数的特殊情况）
     * StringUtils.isNumeric("ab2c") = false
     * StringUtils.isNumeric("12-3") = false
     * StringUtils.isNumeric("12.3") = false
     * StringUtils.isNumeric("12.3") = true
     *
     * @param str
     * @return
     */
    public static boolean isDecimal(String str) {
        if (StringUtils.isBlank(str)) {
            return false;
        }
        Pattern pattern = Pattern.compile("-?[0-9]+\\.*[0-9]*");
        return pattern.matcher(str).matches();
    }

    /**
     * 功能说明 ：是否是小数 最大长度maxBit
     *
     * @return
     * @author Zain 2016/10/18 16:50
     * @params
     */
    public static boolean isDecimal(String str, int maxBit) {
        if (StringUtils.isBlank(str)) {
            return false;
        }
        Pattern pattern = Pattern.compile("-?[0-9]+\\.*[0-9]{0," + maxBit + "}");
        return pattern.matcher(str).matches();
    }

    /**
     * 功能说明 : 判断字符串是否是不包含非空白字符零次或多次
     *
     * @param str
     * @return
     */
    public static boolean isNotBlankStr(String str) {
        if (null == str || StringUtils.isBlank(str)) {
            return false;
        }
        Pattern pattern = Pattern.compile("[\\S]+");
        return pattern.matcher(str).matches();
    }

    /**
     * 功能说明 : 判断字符串是否是不包含非空白字符零次或多次
     *
     * @param str
     * @return
     */
    public static boolean isNotBlankStr(String str, int min, int max) {
        if (null == str || StringUtils.isBlank(str)) {
            return false;
        }
        Pattern pattern = Pattern.compile("[\\S]{" + min + "," + max + "}");
        return pattern.matcher(str).matches();
    }

    /**
     * 功能说明 : 判断对象是否为空，如果空返回""
     *
     * @param obj
     * @return
     */
    public static String nvl(Object obj) {
        return StringTools.nvl(obj, "");
    }

    /**
     * 功能说明 : 判断对象是否为空，如果空返回defaultStr
     *
     * @param obj
     * @param defaultStr
     * @return
     */
    public static String nvl(Object obj, String defaultStr) {
        if (null == obj) {
            return defaultStr;
        }

        String str = obj.toString();

        return StringUtils.isBlank(str) ? defaultStr : str;
    }

    /**
     * 功能说明 : 对象转Int
     *
     * @param obj
     * @return
     */
    public static int obj2Int(Object obj) {
        String str = StringTools.nvl(obj);

        if (StringUtils.isBlank(str)) {
            return 0;
        }

        if (StringUtils.isNotBlank(str) && isDecimal(str)) {
            return Integer.parseInt(str);
        }

        return 0;
    }

    /**
     * 功能说明 : 对象转long
     *
     * @param obj
     * @return
     */
    public static long obj2Long(Object obj) {
        String str = StringTools.nvl(obj);

        if (StringUtils.isNotBlank(str) && StringUtils.isNumeric(str)) {
            return Long.parseLong(str);
        }
        return 0L;
    }

    /**
     * 功能说明 : 对象转Double
     *
     * @param obj
     * @return
     */
    public static double obj2Doble(Object obj) {
        String str = StringTools.nvl(obj);

        if (isDecimal(str)) { //转换为double要校验是否是小数
            return Double.parseDouble(str);
        }
        return 0L;
    }

    /**
     * 功能说明 : 将多个string合并为一个字符串
     *
     * @param div
     * @param strs
     * @return
     */
    public static String packString(String div, String... strs) {
        StringBuffer sb = new StringBuffer();
        for (String str : strs) {
            sb.append(str).append(div);
        }
        return sb.toString().toUpperCase().substring(0, sb.length() - 1);
    }

    /**
     * 功能说明 : 将多个string合并为一个字符串,并添加一个5位随机数
     *
     * @param div
     * @param strs
     * @return
     */
    public static String packStringPlusNumber(String div, String... strs) {
        StringBuffer sb = new StringBuffer();
        for (String str : strs) {
            sb.append(str).append(div);
        }
        return sb.append(genRandomNum(3)).toString().toUpperCase();
    }


    /**
     * 功能说明：去掉BOM头。
     *
     * @param bt
     * @return
     */
    public static byte[] removeBOM(byte[] bt) {
        if (null == bt || bt.length < 3) {
            return bt;
        }

        //EF、BB、BF
        if (bt[0] == -17 && bt[1] == -69 && bt[2] == -65) {
            byte[] nbt = new byte[bt.length - 3];
            System.arraycopy(bt, 3, nbt, 0, nbt.length);
            return nbt;
        } else {
            return bt;
        }
    }

    /**
     * 功能说明：去掉BOM头。
     *
     * @param str
     * @return
     */
    public static String removeBOM(String str) {
        byte[] newByte = null;
        try {
            newByte = StringTools.removeBOM(str.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        String rtnStr = new String(newByte);

        logger.info("rtnStr: " + rtnStr);

        return rtnStr;
    }

    /**
     * 功能说明 ：将字符串采用base64解密得到byte
     *
     * @param str
     * @return
     */
    public static byte[] bASE64Str2Byte(String str) {
        try {
            return new Base64().decode(str.getBytes());
        } catch (Exception e) {
            logger.error("解密base64的字符串为byte失败.", e);
        }
        return null;
    }

    /**
     * 功能说明 ：将字符串转日期
     *
     * @param dateStr
     * @return
     */
    public static Date str2Date(String dateStr) {
        return str2Date(dateStr, false);
    }

    /**
     * 功能说明 ：将字符串转日期,strict为true时为严格校验模式，如不能2月30号
     *
     * @param dateStr
     * @return
     */
    public static Date str2Date(String dateStr, boolean strict) {
        if (StringUtils.isBlank(dateStr)) {
            return null;
        }

        SimpleDateFormat sdf = null;
        if (dateStr.length() == 10) {
            sdf = new SimpleDateFormat("yyyy-MM-dd");
        } else {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }

        if (strict) {
            sdf.setLenient(false);
        }

        try {
            return sdf.parse(dateStr);
        } catch (ParseException e) {
            logger.error("日期字符串解析失败。", e);
        }

        return null;
    }

    /**
     * 功能说明 ：时间转字符串
     *
     * @return
     * @author Zain 2016/10/14 10:33
     * @params
     */
    public static String date2Str(Date dt) {
        return StringTools.date2Str(dt, "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 功能说明 ：时间转指定格式字符串
     *
     * @return
     * @author Zain 2016/10/14 10:33
     * @params
     */
    public static String date2Str(Date dt, String format) {
        if (null == dt) {
            return null;
        }

        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(dt);
    }

    /**
     * 功能说明 : str转Int
     *
     * @param str
     * @return
     */
    public static int str2Int(String str) {
        return StringTools.obj2Int(str);
    }

    /**
     * 功能说明 : str转long
     *
     * @param str
     * @return
     */
    public static long str2Long(String str) {
        return StringTools.obj2Long(str);
    }

    public static float str2Float(String str) {
        if (StringUtils.isNotBlank(str) && isDecimal(str)) {
            return Float.parseFloat(str);
        }
        return 0F;
    }


    /**
     * 功能说明 : 判断字符串是否为一版本串 类似1.1.1
     *
     * @param versionStr
     * @return
     */
    public static boolean isVersionStr(String versionStr) {
        if (StringUtils.isBlank(versionStr)) {
            return false;
        }
        Pattern pattern = Pattern.compile("(\\d)+(\\.\\d+)+");
        return pattern.matcher(versionStr).matches();
    }

    /**
     * 功能说明 : 判断字符串是否为一id连接串1,2,3
     *
     * @param idsStr
     * @return
     */
    public static boolean isIDSStr(String idsStr) {
        if (StringUtils.isBlank(idsStr)) {
            return false;
        }
        Pattern pattern = Pattern.compile("\\d+[,\\d]+");
        return pattern.matcher(idsStr).matches();
    }

    /**
     * 功能说明 : 字符串形式的版本比较   0表示相等;正数表示currVer大于latestVer正常不存在该情况 ;负数表示currVer小于latestVer
     * 版本规则不一致可能报错，外部处理
     *
     * @param currVer
     * @param latestVer
     * @return
     */
    public static int checkVerStr(String currVer, String latestVer) {
        if (currVer.indexOf(".") == -1 && latestVer.indexOf(".") == -1) { //均无下级
            return Long.valueOf(currVer).compareTo(Long.valueOf(latestVer));
        } else if (currVer.indexOf(".") == -1 || latestVer.indexOf(".") == -1) { //当前版本无下级
            return currVer.compareTo(latestVer);
        }
        String currVerSub = currVer.substring(0, currVer.indexOf("."));
        String latestVerSub = latestVer.substring(0, latestVer.indexOf("."));
        if ((Long.valueOf(currVerSub).compareTo(Long.valueOf(latestVerSub))) == 0) {
            return checkVerStr(currVer.substring(currVer.indexOf(".") + 1, currVer.length()),
                    latestVer.substring(latestVer.indexOf(".") + 1, latestVer.length()));
        } else {
            return Long.valueOf(currVerSub).compareTo(Long.valueOf(latestVerSub));
        }
    }

    /**
     * 功能说明 ：计算2个时间的间隔天数
     *
     * @param begin
     * @param end
     * @return
     * @throws Exception
     */
    public static long getDistanceDays(Long begin, Long end) {
        if (null == begin || null == end || begin > end) {
            logger.info("起止时间逻辑错误，存在空或起大于止，begin ：" + begin + "end : " + end);
            return 0;
        }
        return (end - begin) / (1000 * 60 * 60 * 24);
    }

    /**
     * 替换字符串并让它的下一个字母为大写,如将server_desc 转换为serverDesc replacePre为大写字母前的字符
     * *Jmh测试 thrpt   20  10145.305 ± 95.140  ops/ms
     *
     * @param srcStr
     * @param divSign
     * @param replacePre
     * @return
     */
    public static String replaceUnderlineAndfirstToUpper(String srcStr, String divSign, String replacePre) {
        StringBuffer newStringBuffer = new StringBuffer();
        int first = 0; // 标记位置
        while (srcStr.indexOf(divSign) != -1) {
            first = srcStr.indexOf(divSign);
            if (first != srcStr.length()) {
                newStringBuffer = newStringBuffer.append(srcStr.substring(0, first)).append(replacePre);
                srcStr = srcStr.substring(first + divSign.length(), srcStr.length());
                srcStr = srcStr.substring(0, 1).toUpperCase() + srcStr.substring(1); // 首字母大写
            }
        }
        newStringBuffer = newStringBuffer.append(srcStr);
        return newStringBuffer.toString();
    }

    /**
     * 功能说明 ：讲类似将server_desc转换为驼峰形式serverDesc
     * Jmh 测试 thrpt   20   6151.836 ± 63.147  ops/ms
     *
     * @return result
     * @author Zain 2016/7/20  11:42
     * @params
     */
    public static String toCamelCase(String stringWithUnderline) {
        if (stringWithUnderline.indexOf('_') == -1)
            return stringWithUnderline;

        stringWithUnderline = stringWithUnderline.toLowerCase();
        char[] fromArray = stringWithUnderline.toCharArray();
        char[] toArray = new char[fromArray.length];
        int j = 0;
        for (int i = 0; i < fromArray.length; i++) {
            if (fromArray[i] == '_') {
                // 当前字符为下划线时，将指针后移一位，将紧随下划线后面一个字符转成大写并存放
                i++;
                if (i < fromArray.length)
                    toArray[j++] = Character.toUpperCase(fromArray[i]);
            } else {
                toArray[j++] = fromArray[i];
            }
        }
        return new String(toArray, 0, j);
    }

    /**
     * 字符串大写字母转下划线并小写
     *
     * @param srcStr
     * @return
     */
    public static String upperCharToUnderLine(String srcStr, String divSing) {
        Pattern p = Pattern.compile("[A-Z]");
        if (srcStr == null || srcStr.equals("")) {
            return "";
        }
        StringBuffer sb = new StringBuffer(srcStr);
        Matcher mc = p.matcher(srcStr);
        int i = 0;
        while (mc.find()) {
            sb.replace(mc.start() + i, mc.end() + i, divSing + mc.group().toLowerCase());
            i++;
        }

        return sb.toString();
    }

    /**
     * 功能说明 ：map转为form格式 用于http请求
     * author	Zain 2016年7月6日  下午3:50:26
     *
     * @param map
     * @return
     */
    public static String map2FormStr(Map<String, Object> map) {
        if (null == map || map.isEmpty()) {
            return null;
        }
        StringBuffer sb = new StringBuffer();
        for (String key : map.keySet()) {
            sb.append(key).append("=").append(map.get(key)).append("&");
        }
        return sb.toString();
    }

    /**
     * 功能说明 ：判断字符串是否是24小时的hh:mm格式
     *
     * @return
     * @author Zain 2016/9/24 10:48
     * @params
     */
    public static boolean is24hTime(String timeStr) {
        if (StringUtils.isBlank(timeStr)) {
            return false;
        }
        Pattern pattern = Pattern.compile("([01]\\d|2[0-3]):[0-5]\\d");
        return pattern.matcher(timeStr).matches();
    }

    /**
     * 功能说明 ：检查对象是否在数组范围类
     *
     * @return
     * @author Zain 2016/10/31 10:56
     * @params
     */
    public static boolean checkObjRange(Object targetStr, Object... range) {
        return Arrays.asList(range).contains(targetStr);
    }

    /**
     * 功能说明 ： 判断字符串是否在数组范围内
     *
     * @param targetStr
     * @param strArray
     * @return
     * @author Zain 2016/11/28 14:47
     */
    public static boolean isStrInStrArray(String targetStr, String[] strArray) {
        if (null == strArray || strArray.length == 0 || StringUtils.isBlank(targetStr)) {
            return false;
        }
        for (int i = 0; i < strArray.length; i++) {
            if (strArray[i].equals(targetStr)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 功能说明 ：检查对象是否在map中存在并且满足范围
     *
     * @return
     * @author Zain 2016/10/31 10:56
     * @params
     */
    public static <K, V> boolean checkObjInMapAndRange(Map<K, V> map, K key, boolean exist, V... range) {
        V value = map.get(key);
        if (null == value) { //值为空
            if (exist) {//一定要求存在
                return false;
            } else {
                return true;
            }
        }
        return Arrays.asList(range).contains(value);
    }


    /**
     * 功能说明：emoji表情替换。
     *
     * @param source  原字符串
     * @param slipStr emoji表情替换成的字符串
     * @return 过滤后的字符串
     */
    public static String filterEmoji(String source, String slipStr) {
        if (StringUtils.isBlank(source)) {
            return source;
        }

        return source.replaceAll("[\\ud800\\udc00-\\udbff\\udfff\\ud800-\\udfff]", slipStr);
    }

    /**
     * 功能说明：使用给定的分隔符格式化MAC地址。
     *
     * @param macNoSeparator 无分隔符的MAC地址
     * @param separator      分隔符
     * @return 格式化后的MAC地址
     */
    public static String formatMac(String macNoSeparator, String separator) {
        if (StringUtils.isEmpty(macNoSeparator)) {
            logger.warn(String.format("源MAC地址：%s 为空，直接返回。", macNoSeparator));
            return macNoSeparator;
        }

        if (macNoSeparator.contains(separator)) {
            logger.warn(String.format("源MAC地址：%s 包含分隔符：%s，直接返回。", macNoSeparator, separator));
            return macNoSeparator;
        }

        if (macNoSeparator.length() != 12) {
            logger.warn(String.format("源MAC地址：%s 长度有误，直接返回。", macNoSeparator));
            return macNoSeparator;
        }


        String[] macArray = new String[6];
        for (int i = 0; i <= 5; i++) {
            macArray[i] = macNoSeparator.substring(i * 2, i * 2 + 2);
        }

        StringBuilder sbMac = new StringBuilder(macArray[0]);
        for (int i = 1; i < macArray.length; i++) {
            sbMac.append(separator);
            sbMac.append(macArray[i]);
        }

        return sbMac.toString();
    }
}
