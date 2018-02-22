package cn.zain.mr.flow;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN：MR提供的程序读到一行数据的起始偏移量 Long
 * VALUEIN：MR提供的程序读到一行数据的内容 String
 * <p>
 * KEYOUT：用户逻辑处理方法处理完成之后返回给框架数据Key的类型，业务决定 统计基于用户则String
 * VALUEOUT：用户逻辑处理方法处理完成之后返回给框架数据Value的类型，业务决定 统计则为Integer
 * <p>
 * 但数据在网络中传输经常需要序列化，而原生类型序列化，hadoop中不能直接使用，
 * 故一般用Hadoop的LongWritable、Text、IntWritable，序列化精简
 * <p>
 * Created by yongz on 2018/2/21.
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //空格隔开字符隔开统计字符数
        String line = value.toString();
        String[] words = line.split(" ");
        String ip = words[0];
        long upFlow = Long.parseLong(words[1]);
        long downFLow = Long.parseLong(words[2]);
        FlowBean flowBean = new FlowBean(upFlow, downFLow);
        context.write(new Text(ip),flowBean);
    }
}
