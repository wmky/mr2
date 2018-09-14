package com.leyoujia.mr2;

/**
 * Created by KevinYou on 2018-09-06
 * http://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 */

import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import com.leyoujia.util.JsonUtil;


public class AppClientOldLog {
    private static Logger logger = LogManager.getLogger(AppClientOldLog.class);

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
                String json = JsonUtil.getJson(value.toString()).replace("\\n","");
                // TODO 注意json key对应的value是数值类型还是String类型或者Object等.否则ColumnChange中return会报错
                JsonObject jsonObj =jsonParser.parse(json.trim()).getAsJsonObject();
                StringBuffer columns = new StringBuffer();
                columns.append(ColumnChange(jsonObj,"aid"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"bi"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"eis"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"evs"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"it"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"mi"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"pis"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"ssid"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"uuid"));
                columns.append(SpecialChar);
                columns.append(ColumnChange(jsonObj,"ver"));
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
            if (column.equals("bi") || column.equals("mi") ){
                res = jsonObj.has(column) && !jsonObj.get(column).isJsonNull() && !Strings.isNullOrEmpty(jsonObj.get(column).getAsJsonObject().toString()) ? jsonObj.get(column).getAsJsonObject().toString() : EMPTY;
            } else if (column.equals("eis") || column.equals("evs") || column.equals("pis")){
                res = jsonObj.has(column) && !jsonObj.get(column).isJsonNull() && !Strings.isNullOrEmpty(jsonObj.get(column).getAsJsonArray().toString()) ? jsonObj.get(column).getAsJsonArray().toString() : EMPTY;
            } else {
                res = jsonObj.has(column) && !jsonObj.get(column).isJsonNull() && !Strings.isNullOrEmpty(jsonObj.get(column)
                        .getAsString().trim()) ? jsonObj.get(column).getAsString().trim() : EMPTY;
            }
            return res;
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AppClientOldLog");
        job.setJarByClass(AppClientOldLog.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

