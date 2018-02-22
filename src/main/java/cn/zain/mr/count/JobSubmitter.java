package cn.zain.mr.count;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * yarn 的客户端，与yarn通信，让yarn把jar分发到nodemanager上执行
 * Created by yongz on 2018/2/21.
 */
public class JobSubmitter {
    private static Logger logger = Logger.getLogger(JobSubmitter.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        logger.info("start JobSubmitter...");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJar("/home/zain/soft/count/mapreduce-release.jar");
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class); //普通文本,
//        FileInputFormat.setInputPaths(job,new Path("/data/wc/input"));//TextInputFormat的父类
        FileInputFormat.setInputPaths(job,new Path(args[1]));//TextInputFormat的父类


        job.setOutputFormatClass(TextOutputFormat.class);
//        FileOutputFormat.setOutputPath(job,new Path("/data/wc/output"));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        job.setNumReduceTasks(Integer.parseInt(args[0]));

        boolean waitForCompletion = job.waitForCompletion(true);//集群在客户端打印进度

        System.exit(waitForCompletion?0:1); //shell脚本用
    }
}
