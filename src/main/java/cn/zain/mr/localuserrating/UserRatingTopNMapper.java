package cn.zain.mr.localuserrating;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * Created by yongz on 2018/2/23.
 */
public class UserRatingTopNMapper extends Mapper<LongWritable,Text,Text,RateBean> {
    ObjectMapper objectMapper = new ObjectMapper();
    Text text = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //{"movie":"517","rate":"1","timestamp":"1519384662880","uid":"123"}
        RateBean rateBean = objectMapper.readValue(value.toString(), RateBean.class);
        text.set(rateBean.getUid());
        context.write(text,rateBean);
    }
}
