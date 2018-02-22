package cn.zain.mr.count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN, VALUEIN map阶段输出的数据key和value的类型
 *
 *  KEYOUT, VALUEOUT 逻辑处理后的结果
 *
 * Created by yongz on 2018/2/21.
 */
public class CountReducer extends Reducer<Text, IntWritable,Text, IntWritable> {
    /**
     * MR框架的reduce端程序在处理好一组相同key数据后，调用一次reduce
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values){
            count += value.get();
        }
        context.write(key,new IntWritable(count));
    }
}
