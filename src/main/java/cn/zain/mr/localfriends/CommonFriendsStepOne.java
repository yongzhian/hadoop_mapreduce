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
public class CommonFriendsStepOne {
    static class CommonFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text k = new Text();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //寒锦:采雪,橘帆,月婧,函玉,花慧,洁璇,雪月,彩柏
            String line = value.toString();
            String[] split = line.split(":");
            String user = split[0];
            String[] friends = split[1].split(",");
            //先构成寒锦:采雪  寒锦:橘帆等，
            v.set(user);
            for (int i = 0; i < friends.length; i++) {
                k.set(friends[i]);
                context.write(k, v);
            }

        }
    }

    static class CommonFriendsStepOneReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text friend, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //在第一步的reduce中得到共同好友下所有的好友
            ArrayList<Text> users = new ArrayList<>();
            for (Text user : values) {
                users.add(new Text(user));
            }
            Collections.sort(users);
            for (int i = 0; i < users.size() - 1; i++) {
                for (int j = i + 1; j < users.size(); j++) {
                    context.write(new Text(users.get(i) + "-" + users.get(j)), friend);
                }
            }
        }
    }

    private static Logger logger = Logger.getLogger(UserRatingTopN.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        logger.info("启动方式：hadoop jar xx.jar 3 /home/zain/input /honme/zain/output");
        logger.info("start local CommonFriendsStepOne...");
        Configuration conf = new Configuration();

        conf.set("mapreduce.framework.name", "local");
//        conf.set("fs.defaultFS", "hdfs://vm1.zain.cn:9000"); 不写默认为file:///，寻找本地路径，而通过hadoop jar会找hdfs目录

        Job job = Job.getInstance(conf);
        job.setJarByClass(CommonFriendsStepOne.class);

        job.setMapperClass(CommonFriendsStepOneMapper.class);
        job.setReducerClass(CommonFriendsStepOneReducer.class);

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
