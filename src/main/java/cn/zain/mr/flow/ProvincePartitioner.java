package cn.zain.mr.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * 按ip的省份进行分发
 * Created by yongz on 2018/2/22.
 */
public class ProvincePartitioner extends Partitioner<Text,FlowBean> {
    protected static HashMap<String,Integer> provinceCode = new HashMap<String, Integer>();
    static {
        provinceCode.put("131",0);
        provinceCode.put("132",1);
        provinceCode.put("133",2);
        provinceCode.put("134",3);
        provinceCode.put("135",4);
        provinceCode.put("136",5);
        provinceCode.put("137",6);
        provinceCode.put("138",7);
    }
    public int getPartition(Text text, FlowBean flowBean, int i) {
        Integer code = provinceCode.get(text.toString().substring(0,3));
        return code = null==code?4:code;
    }
}
