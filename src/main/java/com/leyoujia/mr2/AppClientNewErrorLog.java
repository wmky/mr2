package com.leyoujia.mr2;


/**
 * Created by wmky_kk on 2017-08-21.
 * http://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 */

import com.leyoujia.util.ArrUtil;
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


public class AppClientNewErrorLog {
    private static Logger logger = LogManager.getLogger(AppClientNewErrorLog.class);
    public static class TokenizerMapper
            extends Mapper<Object, Text, NullWritable, Text>{

        static enum CountersEnum { IRREGULAR_INPUT_LOGS,REGULAR_INPUT_LOGS };

        private String SpecialChar = "\u0001";
        private String SplitChar = "\\|\\|";

        private Configuration conf;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            /**
             * JobContext接口中抽象方法
             * Return the configuration for the job.
             * @return the shared configuration object  共享配置
             */
            conf = context.getConfiguration();

        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Text rValue = new Text();
            String line = value.toString();
            String[] arr =  line.split(SplitChar);
            if ( arr.length == 19 ){
                String itComplex = arr[0];
                String it =itComplex.contains("\u0000") ? itComplex.split("\u0000")[itComplex.split("\u0000").length-1]:itComplex;
                String ip = arr[1];
                String logsource = arr[2];
                String uuid = arr[3];
                String aid = arr[4];
                String ssid = arr[5];
                String ver = arr[6];
                String ost = arr[7];
                String model = arr[8];
                String bi_nt = arr[9];
                String bi_np = arr[10];
                String bi_lng = arr[11];
                String bi_lat = arr[12];
                String channel = arr[13];
                String mac = arr[14];
                String imei = arr[15];
                String idfa = arr[16];
                String imsi = arr[17];
                String uid = ArrUtil.ArrToMap(arr[18]).get("uid");
                String np = ArrUtil.ArrToMap(arr[18]).get("np");
                String bd = ArrUtil.ArrToMap(arr[18]).get("bd");
                String osv = ArrUtil.ArrToMap(arr[18]).get("osv");
                String err = ArrUtil.ArrToMap(arr[18]).get("err");
                String pe = ArrUtil.ArrToMap(arr[18]).get("pe");
                String erId = ArrUtil.ArrToMap(arr[18]).get("erId");
                String type = ArrUtil.ArrToMap(arr[18]).get("type");
                String bua = ArrUtil.ArrToMap(arr[18]).get("bua");

                StringBuffer columns = new StringBuffer();
                columns.append(it);
                columns.append(SpecialChar);
                columns.append(ip);
                columns.append(SpecialChar);
                columns.append(logsource);
                columns.append(SpecialChar);
                columns.append(uuid);
                columns.append(SpecialChar);
                columns.append(aid);
                columns.append(SpecialChar);
                columns.append(ssid);
                columns.append(SpecialChar);
                columns.append(ver);
                columns.append(SpecialChar);
                columns.append(ost);
                columns.append(SpecialChar);
                columns.append(model);
                columns.append(SpecialChar);
                columns.append(bi_nt);
                columns.append(SpecialChar);
                columns.append(bi_np);
                columns.append(SpecialChar);
                columns.append(bi_lng);
                columns.append(SpecialChar);
                columns.append(bi_lat);
                columns.append(SpecialChar);
                columns.append(channel);
                columns.append(SpecialChar);
                columns.append(mac);
                columns.append(SpecialChar);
                columns.append(imei);
                columns.append(SpecialChar);
                columns.append(idfa);
                columns.append(SpecialChar);
                columns.append(imsi);
                columns.append(SpecialChar);
                columns.append(uid);
                columns.append(SpecialChar);
                columns.append(np);
                columns.append(SpecialChar);
                columns.append(bd);
                columns.append(SpecialChar);
                columns.append(osv);
                columns.append(SpecialChar);
                columns.append(err);
                columns.append(SpecialChar);
                columns.append(pe);
                columns.append(SpecialChar);
                columns.append(erId);
                columns.append(SpecialChar);
                columns.append(type);
                columns.append(SpecialChar);
                columns.append(bua);
                rValue.set(columns.toString());
                context.write(NullWritable.get(),rValue);
                Counter counterRegular = context.getCounter(CountersEnum.class.getName(),
                        CountersEnum.REGULAR_INPUT_LOGS.toString());
                counterRegular.increment(1);
            } else {
                logger.info(line);
                Counter counterIrregular = context.getCounter(CountersEnum.class.getName(),
                CountersEnum.IRREGULAR_INPUT_LOGS.toString());
                counterIrregular.increment(1);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException,InterruptedException{

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AppClientNewErrorLog");
        job.setJarByClass(AppClientNewErrorLog.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
/*        String line = "2018-08-23 00:00:13||219.133.157.225||0||oVs0Z0d2BVbQfSL7ChBXnX56Vc_k||MINI001||-||1.4.0||Android||Redmi 5 Plus||wifi||0||114.132355||22.611298||-||-||-||-||-||eId=M78602240&obj={\"cityCode\":\"000002\",\"fhId\":\"1030933\"}&bd=xiaomi&osv=7.1.2&uid=";
        String[] arr = line.split("\\|\\|");
        System.out.println(arr.length);  // arr 长度为19*/
    }
}
