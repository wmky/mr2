package com.leyoujia.util;

import jdk.nashorn.internal.runtime.regexp.joni.Regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class JsonUtil {
    public static String getJson(String value) {
        String rawLine = value.toString();
        String json = "";
        if (rawLine.substring(rawLine.length() - 1, rawLine.length()).equals("]") ||
                rawLine.substring(rawLine.length() - 1, rawLine.length()).equals("}")) {
            if (rawLine.indexOf("[") >= 0 && rawLine.indexOf("]") >= 0
                    && (rawLine.indexOf("[") < rawLine.indexOf("{"))) {
                json = rawLine.substring(rawLine.indexOf("["),
                        rawLine.length());
            } else if (rawLine.indexOf("{") >= 0 && rawLine.indexOf("}") > 0) {
                json = rawLine.substring(rawLine.indexOf("{"),
                        rawLine.length());
            }
        }
        return json;
    }

    public static void main(String args[]) {
        String value = "./tongji_170308.log.2018-09-090000644000000000000002034207603413345241577013635 0ustar 2019-09-09 00:00:01 rootroot2018-09-09 00:00:00 {\"ip\":\"14.155.89.25\",\"uid\":\"7b57d0f3-7475-4e34-8a4d-f82ddf9fcf54\",\"sid\":\"DC62A58041497D4514ACD22E0A7200D6\",\"pid\":\"b771fbdd.5da5d4ac\",\"loc\":\"https://wap.leyoujia.com/shenzhen/esf/?c=%E9%87%91%E7%A2%A7%E8%8B%91\",\"ref\":\"https://wap.leyoujia.com/shenzhen/esf/\",\"nua\":\"Mozilla/5.0 (Linux; Android 7.0; EVA-AL10 Build/HUAWEIEVA-AL10) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Mobile Safari/537.36\",\"scs\":\"360*640\",\"ver\":\"JS-V2.0.9\",\"utm\":\"222sm0000102412\"}";
        System.out.println(getJson(value));
        Pattern pattern = Pattern.compile("(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2})");
        Matcher m = pattern.matcher(value);
        String dateStr = "";
        while (m.find()) {
            dateStr += m.group() + "\u0001";
        }
        System.out.println(dateStr.split("\u0001")[0]);
    }
}
