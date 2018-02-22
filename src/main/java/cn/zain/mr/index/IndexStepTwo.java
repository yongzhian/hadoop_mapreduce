package cn.zain.mr.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by yongz on 2018/2/22.
 */
public class IndexStepTwo {
    private static class IndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * hello-a.txt	3
         jim-a.txt	1
         tom-a.txt	1
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("-");
            String word = split[0];
            String[] tmp = split[1].split("\t");
            String fileName = tmp[0];
            String count = tmp[1];
            context.write(new Text(word), new Text(fileName+"-->"+count));
        }
    }

    private static class IndexStepTwoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value.toString()).append(" ");
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    private static Logger logger = Logger.getLogger(IndexStepTwo.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        logger.info("start IndexStepTwo...");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(IndexStepTwo.class); //动态jar

        job.setMapperClass(IndexStepTwoMapper.class);
        job.setReducerClass(IndexStepTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //如果map与reduce阶段kv类型一样，则map阶段可不写（上2行）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(Integer.parseInt(args[0]));

        job.setInputFormatClass(TextInputFormat.class); //普通文本,默认TextInputFormat，可不写
        FileInputFormat.setInputPaths(job, new Path(args[1]));//TextInputFormat的父类


        job.setOutputFormatClass(TextOutputFormat.class);//普通文本,默认TextInputFormat，可不写
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean waitForCompletion = job.waitForCompletion(true);//集群在客户端打印进度

        System.exit(waitForCompletion ? 0 : 1); //shell脚本用
    }
}
