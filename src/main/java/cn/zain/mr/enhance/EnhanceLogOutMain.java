package cn.zain.mr.enhance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by yongz on 2018/2/27.
 */
public class EnhanceLogOutMain {
    private static Logger logger = Logger.getLogger(EnhanceLogOutMain.class);

    static class EnhanceLogOutMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            for (String word : words) {
                text.set(word);
                context.write(text, NullWritable.get());
            }
        }
    }

    static class EnhanceLogOutReducer extends Reducer<Text, Text, Text, NullWritable> {
        private MultipleOutputs<Text, NullWritable> mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (key.toString().startsWith("a")) {
                mos.write( key, NullWritable.get(), "a/");
            } else if (key.toString().startsWith("b")) {
                mos.write( key, NullWritable.get(), "b/");
            } else {
                mos.write( key, NullWritable.get(), "other/");
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(EnhanceLogOutMain.class);

        job.setMapperClass(EnhanceLogOutMapper.class);
        job.setReducerClass(EnhanceLogOutReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        LazyOutputFormat.setOutputFormatClass(job,TextOutputFormat.class); //可以不产生空partXX文件

//        MultipleOutputs.addNamedOutput(job, "A", TextOutputFormat.class, Text.class, NullWritable.class);
//        MultipleOutputs.addNamedOutput(job, "B", TextOutputFormat.class, Text.class, NullWritable.class);
//        MultipleOutputs.addNamedOutput(job, "Other", TextOutputFormat.class, Text.class, NullWritable.class);
        //如果有上面定义别名，则mos.write("Other", key, NullWritable.get(), "other/");需要指明
        job.setNumReduceTasks(1);

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}
