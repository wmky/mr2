package com.leyoujia.mr2;

/**
 * Created by KevinYou on 2018-09-06
 * http://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 */

import java.io.IOException;
import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.leyoujia.util.JsonUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;



public class PcWapServerLog {
    private static Logger logger = LogManager.getLogger(PcWapServerLog.class);

    public static class TokenizerMapper
            extends Mapper<Object, Text, NullWritable, Text> {

        static enum CountersEnum {IRREGULAR_INPUT_LOGS, REGULAR_INPUT_LOGS}
        private static final JsonParser jsonParser = new JsonParser();
        private String SpecialChar = "\u0001";
        private String EMPTY = "";
        private Configuration conf;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();

        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Text rValue = new Text();
            try{
                String json = JsonUtil.getJson(value.toString());
                // TODO 注意json key对应的value是数值类型还是String类型或者Object等.否则ColumnChange中return会报错
                JsonObject jsonObj =jsonParser.parse(json.trim()).getAsJsonObject();
                StringBuffer columns = new StringBuffer();
                columns.append(ColumnChange(jsonObj,"accessAddress"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"cityCode"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"cookiesId"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"execTime"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"ip"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"jjshomeUuid"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"keyWord"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"logType"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"phone"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"qq"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"refererAddress"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"requestMap"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"resultMap"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"sessionId"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"status"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"tags"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"userAgent"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"userId"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"viewName"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"wbId"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"wcNO"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"winxin"));
                columns.append(SpecialChar);
                rValue.set(columns.toString());
                context.write(NullWritable.get(),rValue);
                Counter counterRegular = context.getCounter(CountersEnum.class.getName(),
                        CountersEnum.REGULAR_INPUT_LOGS.toString());
                counterRegular.increment(1);
            }catch (Exception e){
                logger.error(value.toString() + "\n" + e );
                Counter counterIrregular = context.getCounter(CountersEnum.class.getName(),
                        CountersEnum.IRREGULAR_INPUT_LOGS.toString());
                counterIrregular.increment(1);
            }
        }

        private String ColumnChange(JsonObject jsonObj, String column) {
            String res ="";
            res = jsonObj.has(column)&& !jsonObj.get(column).isJsonNull()&& !Strings.isNullOrEmpty(jsonObj.get(column).toString())
                    ? jsonObj.get(column).toString() : EMPTY;
            return res;
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PcWapServerLog");
        job.setJarByClass(PcWapServerLog.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

/*      String line = "./big-data-zf.log.2018-09-050000644000000000000000157462455213343776575013625 0ustar  rootroot2018-09-05 00:00:00 ZfListController@BigDataLog{\"accessAddress\":\"http://zhengzhou.leyoujia.com/zf/?n=2\",\"cityCode\":\"004778\",\"cookiesId\":\"ba329cd1724d48e28fa5a632008d2cbd\",\"execTime\":\"2018-09-05 00:00:00\",\"ip\":\"112.9.99.127\",\"jjshomeUuid\":\"\",\"keyWord\":\"\",\"logType\":\"ZfListController@BigDataLog\",\"phone\":\"\",\"qq\":\"\",\"refererAddress\":\"\",\"requestMap\":{\"queryBean\":{\"areaCode\":\"\",\"areaEnd\":null,\"areaIds\":\"\",\"areaIndex\":\"\",\"areaName\":\"\",\"areaStart\":null,\"areaTags\":\"\",\"attributes\":{},\"cityCode\":\"004778\",\"cityName\":\"郑州\",\"comId\":null,\"comIds\":[],\"comName\":\"\",\"distance\":null,\"ditieQuery\":null,\"fitments\":\"\",\"floors\":\"\",\"forwards\":\"\",\"houseAges\":\"\",\"houseId\":null,\"houseIds\":[],\"includeFrontImage\":true,\"includeMetaInfo\":true,\"includeZrf\":false,\"isAlbum\":null,\"isNearSubway\":null,\"isschool\":null,\"keyword\":\"\",\"lat\":null,\"latEnd\":null,\"latStart\":null,\"lngEnd\":null,\"lngStart\":null,\"lon\":null,\"orderField\":null,\"pageNum\":2,\"pageNumLimit\":100,\"pageSize\":20,\"pageSizeLimit\":100,\"placeCode\":\"\",\"placeIndex\":\"\",\"placeName\":\"\",\"priceEnd\":null,\"priceIds\":\"\",\"priceStart\":null,\"priceTags\":\"\",\"propertyTypes\":\"\",\"pubdateId\":\"\",\"pubdateName\":\"\",\"rentalWay\":\"\",\"rentalWayName\":\"\",\"rooms\":\"\",\"schoolId\":null,\"schoolName\":\"\",\"status\":null,\"subStationId\":null,\"subStationName\":\"\",\"subWayId\":null,\"subWayName\":\"\",\"suggestKey\":\"\",\"tags\":\"\",\"workerId\":\"\"},\"n\":\"2\"},\"resultMap\":{\"resultCount\":57},\"sessionId\":\"65C325D568A64F89DD1E799AFCDBE019\",\"status\":\"200\",\"tags\":\"\",\"userAgent\":\"Mozilla/5.0 (Windows NT 6.3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36\",\"userId\":\"\",\"viewName\":\"zf/zf-list\",\"wbId\":\"\",\"wcNO\":\"\",\"winxin\":\"\"}";
        //输入的字符串最后一个位置的值是否是“]”或者"}"
        String json = line;
        if (json.substring(json.length() - 1, json.length()).equals("]") ||
                json.substring(json.length() - 1, json.length()).equals("}")) {
            if (json.indexOf("[") >= 0 && json.indexOf("]") >= 0
                    && (json.indexOf("[") < json.indexOf("{"))) {
                json = json.substring(json.indexOf("["),
                        json.length());
            } else if (json.indexOf("{") >= 0 && json.indexOf("}") > 0) {
                json = json.substring(json.indexOf("{"),
                        json.length());
            }
        }
        System.out.println("leyoujia" + json);
        System.out.println(ColumnChange(json,"accessAddress"));*/
    }
/*    public static String ColumnChange(String json,String column){
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObj = null;
        String x = "";
        try{
            System.out.println("xx" + json);
         jsonObj =(JsonObject) jsonParser.parse(json);
             x = jsonObj.has(column)&& !jsonObj.get(column).isJsonNull()&& !Strings.isNullOrEmpty(jsonObj.get(column)
                    .getAsString().trim()) ?jsonObj.get(column).getAsString().trim() : "" ;
        }
        catch (Exception e){
            System.out.println(e+"abc");
        }
        return x;
    }*/
}

