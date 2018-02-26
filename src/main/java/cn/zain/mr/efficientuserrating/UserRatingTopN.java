package cn.zain.mr.efficientuserrating;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by yongz on 2018/2/25.
 */
public class UserRatingTopN {
    static ObjectMapper objectMapper = new ObjectMapper();
    Text t = new Text();

    static class UserRatingTopNMapper extends Mapper<LongWritable, Text, RateBean, NullWritable> {
        HashMap<String,String> map = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            DBLoader.load(map);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Counter counter = context.getCounter("group1", "name1");//全局计计数器
            try{

            RateBean bean = objectMapper.readValue(value.toString(), RateBean.class);
            if("10".equals(bean.getRate())){
                bean.setMovie(map.get("10")+ bean.getMovie());
            }

            context.write(bean, NullWritable.get());}
            catch (Exception e){
                counter.increment(1);//出现异常计数器+1
            }
        }
    }

    static class UserRatingTopNReducer extends Reducer<RateBean, NullWritable, RateBean, NullWritable> {
        @Override
        protected void reduce(RateBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int topN = context.getConfiguration().getInt("rate.top.n", 3);
            int count = 0;
            for (NullWritable nullWritable : values) {
                context.write(key, NullWritable.get());
                count++;
                if (count == topN) {
                    return;
                }
            }
        }
    }


    private static Logger logger = Logger.getLogger(UserRatingTopN.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        logger.info("启动方式：hadoop jar xx.jar 3 /home/zain/input /honme/zain/output");
        logger.info("start local UserRatingTopN...");
        Configuration conf = new Configuration();
        conf.addResource("userconf.xml");

        Job job = Job.getInstance(conf);
        job.setJarByClass(UserRatingTopN.class);

        job.setMapperClass(UserRatingTopNMapper.class);
        job.setReducerClass(UserRatingTopNReducer.class);

        job.setOutputKeyClass(RateBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setPartitionerClass(UidPartitioner.class);
        job.setGroupingComparatorClass(UidGroupingComparator.class);

        job.setOutputFormatClass(EnhanceOutputFormat.class);
        //并不写
        FileInputFormat.setInputPaths(job, new Path(args[1]));//TextInputFormat的父类
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setNumReduceTasks(Integer.parseInt(args[0]));

        boolean waitForCompletion = job.waitForCompletion(true);//集群在客户端打印进度

        System.exit(waitForCompletion ? 0 : 1); //shell脚本用
    }
}
