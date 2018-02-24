package cn.zain.mr.localuserrating;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by yongz on 2018/2/23.
 */
public class UserRatingTopN {
    private static Logger logger = Logger.getLogger(UserRatingTopN.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        logger.info("启动方式：hadoop jar xx.jar 3 /home/zain/input /honme/zain/output 5");//最后一个是前5
        logger.info("start local UserRatingTopN...");
        Configuration conf = new Configuration();

        conf.set("user.rate.topn",args[3]); //参数
        conf.set("mapreduce.framework.name", "local");
//        conf.set("fs.defaultFS", "hdfs://vm1.zain.cn:9000"); 不写默认为file:///，寻找本地路径，而通过hadoop jar会找hdfs目录

        Job job = Job.getInstance(conf);
        job.setJarByClass(UserRatingTopN.class);

        job.setMapperClass(UserRatingTopNMapper.class);
        job.setReducerClass(UserRatingTopNReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RateBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(RateBean.class);

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
