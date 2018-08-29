package cn.jpush.util;

import java.util.HashMap;
import java.util.Map;

public class ArrUtil {
    /**ArrToMap
     * 将传入的数组转化为key-value的xing
     * @param
     */
    public static Map<String,String> ArrToMap(String complexStr) {
        //去掉换行符
        String key = "";
        String value = "";
        Map<String,String> map = new HashMap<String,String>();
        String[] arr = complexStr.split("\\&");
        for (int i=0;i<arr.length;i++) {
            //判断字符串中有没有点等于号
            if(arr[i].length()>0 &&arr[i] !=null&&arr[i]!=""){
                String a = arr[i]+"%";
                if(arr[i].indexOf("=") >= 0 ){
                    key = a.substring(0,a.indexOf("=")).trim();
                    value = a.substring(a.indexOf("=")+1,a.indexOf("%")).trim();
                }
                map.put(key,value);
            }
        }
        return map;
    }
    public static void main(String[] args){
//        HashMap<String,String> map = new HashMap<String,String>();
        String line = "eId=M70211584&obj={\"cityCode\":\"000002\",\"priceTags\":\"3000-6000\",\"rooms\":\"2\",\"includeFrontImage\":1,\"pageNo\":1,\"pageSize\":20}&bd=iPhone&osv=10.3.1&uid=";
        Map<String,String> map = ArrToMap(line);
        System.out.println("eId = " + map.get("eId"));
        System.out.println("obj = " + map.get("obj"));
        System.out.println("bd = " + map.get("bd"));
        System.out.println("osv = " + map.get("osv"));
        System.out.println("uid = " + map.get("uid"));
    }
}
