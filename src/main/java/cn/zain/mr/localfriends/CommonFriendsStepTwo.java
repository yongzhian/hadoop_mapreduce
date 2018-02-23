package cn.zain.mr.localfriends;

import cn.zain.mr.localuserrating.UserRatingTopN;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by yongz on 2018/2/23.
 */
public class CommonFriendsStepTwo {
    static class CommonFriendsStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text k = new Text();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //B-D	A
            String line = value.toString();
            String[] split = line.split("\t");
            String userPair = split[0];
            String friend = split[1];
            k.set(userPair);
            v.set(friend);
            context.write(k, v);
        }
    }

    static class CommonFriendsStepTwoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text userPair, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text text : values) {
                sb.append(text).append(",");
            }
            context.write(new Text(userPair),new Text(sb.substring(0,sb.length()-1)));
        }
    }

    private static Logger logger = Logger.getLogger(UserRatingTopN.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        logger.info("启动方式：hadoop jar xx.jar 3 /home/zain/input /honme/zain/output");
        logger.info("start local CommonFriendsStepTwo...");
        Configuration conf = new Configuration();

        conf.set("mapreduce.framework.name", "local");
//        conf.set("fs.defaultFS", "hdfs://vm1.zain.cn:9000"); 不写默认为file:///，寻找本地路径，而通过hadoop jar会找hdfs目录

        Job job = Job.getInstance(conf);
        job.setJarByClass(CommonFriendsStepTwo.class);

        job.setMapperClass(CommonFriendsStepTwoMapper.class);
        job.setReducerClass(CommonFriendsStepTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class); //普通文本,
//        FileInputFormat.setInputPaths(job,new Path("/data/wc/input"));//TextInputFormat的父类
        FileInputFormat.setInputPaths(job, new Path(args[1]));//TextInputFormat的父类


        job.setOutputFormatClass(TextOutputFormat.class);
//        FileOutputFormat.setOutputPath(job,new Path("/data/wc/output"));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setNumReduceTasks(Integer.parseInt(args[0]));


        boolean waitForCompletion = job.waitForCompletion(true);//集群在客户端打印进度

        System.exit(waitForCompletion ? 0 : 1); //shell脚本用
    }
}
