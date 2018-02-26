package cn.zain.mr.efficientuserrating;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by yongz on 2018/2/25.
 */
public class EnhanceOutputFormat extends FileOutputFormat<RateBean, NullWritable> {
    @Override
    public RecordWriter<RateBean, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSystem client = FileSystem.get(taskAttemptContext.getConfiguration());
        FSDataOutputStream fsDataOutputStream1 = client.create(new Path("/data/efficientuserrating/fullscore/fullscore.txt"));
        FSDataOutputStream fsDataOutputStream2 = client.create(new Path("/data/efficientuserrating/normal/normal.txt"));
        return new EnhanceRecordWriter(fsDataOutputStream1, fsDataOutputStream2);
    }

    static class EnhanceRecordWriter extends RecordWriter<RateBean, NullWritable> {
        private FSDataOutputStream fsDataOutputStream1;
        private FSDataOutputStream fsDataOutputStream2;

        public EnhanceRecordWriter(FSDataOutputStream fsDataOutputStream1, FSDataOutputStream fsDataOutputStream2) {
            this.fsDataOutputStream1 = fsDataOutputStream1;
            this.fsDataOutputStream2 = fsDataOutputStream2;
        }

        @Override
        public void write(RateBean rateBean, NullWritable nullWritable) throws IOException, InterruptedException {
            fsDataOutputStream2.write(rateBean.toString().getBytes());
            if (rateBean.getMovie().contains("fullscore")) {
                //待处理数据
                fsDataOutputStream1.write(rateBean.toString().getBytes());
                fsDataOutputStream1.write("\n".getBytes());
                fsDataOutputStream1.flush();
            }
            fsDataOutputStream2.write(rateBean.toString().getBytes());
            fsDataOutputStream2.write("\n".getBytes());
            fsDataOutputStream2.flush();
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (null != fsDataOutputStream1) {
                fsDataOutputStream1.close();
            }
            if (null != fsDataOutputStream2) {
                fsDataOutputStream2.close();
            }
        }
    }
}
