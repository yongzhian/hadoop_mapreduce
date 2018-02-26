package cn.zain.mr.efficientuserrating;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by yongz on 2018/2/25.
 */
public class UidGroupingComparator extends WritableComparator {

    public UidGroupingComparator() {
        super(RateBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        RateBean key1 = (RateBean) a;
        RateBean key2 = (RateBean) b;
        return key1.getUid().compareTo(key2.getUid());
    }
}
