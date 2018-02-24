package cn.zain.mr.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by yongz on 2018/2/24.
 */
public class JoinSubmitter {
    private static Logger logger = Logger.getLogger(JoinSubmitter.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        logger.info("JoinSubmitter启动方式：hadoop jar xx.jar 3 /home/zain/input /honme/zain/output");//
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf);
        job.setJarByClass(JoinSubmitter.class);

        job.setMapperClass(JoinMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);

        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));//TextInputFormat的父类
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setNumReduceTasks(Integer.parseInt(args[0]));

        boolean waitForCompletion = job.waitForCompletion(true);//集群在客户端打印进度
        System.exit(waitForCompletion ? 0 : 1); //shell脚本用

    }
}
