package cn.zain.mr.efficientuserrating;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by yongz on 2018/2/25.
 */
public class UidPartitioner extends Partitioner<RateBean,NullWritable> {
    @Override
    public int getPartition(RateBean rateBean, NullWritable nullWritable, int numReduceTasks) {
        return (rateBean.getUid().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
