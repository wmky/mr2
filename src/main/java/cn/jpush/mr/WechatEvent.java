package cn.jpush.mr;


/**
 * Created by wmky_kk on 2017-08-21.
 * http://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;


public class WechatEvent {

    public static class TokenizerMapper
            extends Mapper<Object, Text, NullWritable, Text>{

        static enum CountersEnum { INPUT_WORDS }

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

            Text line = value;
                context.write(NullWritable.get(), line);
                // 计数器
                Counter counter = context.getCounter(CountersEnum.class.getName(),
                        CountersEnum.INPUT_WORDS.toString());
                counter.increment(1);
            }
        @Override
        public void cleanup(Context context) throws IOException,InterruptedException{

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "WechatEvent");
        job.setJarByClass(WechatEvent.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
