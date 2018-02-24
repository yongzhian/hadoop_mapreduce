package cn.zain.mr.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by yongz on 2018/2/24.
 */
public class JoinMapper extends Mapper<LongWritable,Text,Text,JoinBean> {
    private String name;

    //一个map程序运行实例在调用自定义mapper逻辑类，首先会调用一次setup方法且只一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //当前切片信息
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();//该切片处理的文件名
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split= line.split(" ");
        JoinBean joinBean = new JoinBean();
        joinBean.setTableName(name);
        if("order.txt".equals(name)){
            joinBean.setOrderId(split[0]);
            joinBean.setUid(split[1]);
        }else{
            joinBean.setUid(split[0]);
            joinBean.setUsername(split[1]);
            joinBean.setAge(split[2]);
            joinBean.setLover(split[3]);
        }
        context.write(new Text(joinBean.getUid()),joinBean);
    }
}
