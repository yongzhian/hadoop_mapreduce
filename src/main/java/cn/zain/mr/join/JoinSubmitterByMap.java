package cn.zain.mr.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

/**
 * 只通过map阶段实现
 * Created by yongz on 2018/2/24.
 */
public class JoinSubmitterByMap {

    private static Logger logger = Logger.getLogger(JoinSubmitterByMap.class);

    static class JoinByMapMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        private HashMap<String,String> userMap = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileReader fr = new FileReader("user.txt");
            BufferedReader br = new BufferedReader(fr);
            String line = "";
            while (StringUtils.isNotBlank(line = br.readLine())){
                String[] split = line.split(" ");
                userMap.put(split[0],split[1] + "\t" + split[2] + "\t" + split[3]);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(" ");
            String userInfo = userMap.get(split[1]);
            context.write(new Text(line + "\t" + userInfo),NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        logger.info("JoinSubmitterByMap启动方式：hadoop jar xx.jar  /home/zain/input /honme/zain/output");//
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"JoinSubmitterByMap");
        job.setJarByClass(JoinSubmitter.class);

        job.setMapperClass(JoinByMapMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));//TextInputFormat的父类
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(0);//只有mapper
        job.addCacheFile(new URI("hdfs:///data/join/cache/user.txt"));//会吧文件发送到工作目录，名称不变

        boolean waitForCompletion = job.waitForCompletion(true);//集群在客户端打印进度
        System.exit(waitForCompletion ? 0 : 1); //shell脚本用

    }
}
