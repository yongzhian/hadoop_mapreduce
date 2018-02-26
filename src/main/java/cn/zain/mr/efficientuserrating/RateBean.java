package cn.zain.mr.efficientuserrating;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yongz on 2018/2/23.
 */
public class RateBean implements WritableComparable<RateBean> {
    private String movie;
    private String rate;
    private String timestamp;
    private String uid;

    public void set(String movie, String rate, String timestamp, String uid) {
        this.movie = movie;
        this.rate = rate;
        this.timestamp = timestamp;
        this.uid = uid;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.movie);
        dataOutput.writeUTF(this.rate);
        dataOutput.writeUTF(this.timestamp);
        dataOutput.writeUTF(this.uid);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.movie = dataInput.readUTF();
        this.rate = dataInput.readUTF();
        this.timestamp = dataInput.readUTF();
        this.uid = dataInput.readUTF();
    }


    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public String getRate() {
        return rate;
    }

    public void setRate(String rate) {
        this.rate = rate;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    @Override
    public String toString() {
        return uid + "  RateBean{" +
                "movie='" + movie + '\'' +
                ", rate='" + rate + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", uid='" + uid + '\'' +
                '}';
    }

    @Override
    public int compareTo(RateBean o) {
        if(this.uid.equals(o.getUid())){
            return -(Integer.parseInt(this.rate) - Integer.parseInt(o.getRate())); //分数大的在前
        }
        return this.uid.compareTo(o.getUid());
    }
}
