package cn.jpush.util;

import java.util.HashMap;
import java.util.Map;

public class ArrUtil {
    /**
     * 将传入的数组转化为key-value的xing
     * @param
     */
    public static void ArrToMap(String complexStr,Map<String,String> map) {
        //去掉换行符
        String key = "";
        String value = "";
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
    }
    public static void main(String[] args){
        HashMap<String,String> map = new HashMap<String,String>();
        String line = "eId=M70211584&obj={\"cityCode\":\"000002\",\"priceTags\":\"3000-6000\",\"rooms\":\"2\",\"includeFrontImage\":1,\"pageNo\":1,\"pageSize\":20}&bd=iPhone&osv=10.3.1&uid=";
        ArrToMap(line,map);
        System.out.println(map.get("eId"));
    }
}
