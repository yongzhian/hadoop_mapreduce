package cn.zain.mr.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * Created by yongz on 2018/2/24.
 */
public class JoinReducer extends Reducer<Text,JoinBean,Text,Text> {
    Text k = new Text();
    Text v= new Text();
    @Override
    protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {

        ArrayList<JoinBean> orders = new ArrayList<>();
        JoinBean newJoinBean = new JoinBean();
        for (JoinBean joinBean:values){
            JoinBean tmp = new JoinBean();
            try {
                BeanUtils.copyProperties(tmp,joinBean);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            if("order.txt".equals(joinBean.getTableName())){
                orders.add(tmp);
            }else{
                newJoinBean = tmp;
            }
        }

        for (JoinBean order:orders){
            k.set(order.getOrderId());
            v.set(order.getUid() + " "  + newJoinBean.getUsername()  + " " + newJoinBean.getAge() + " "  + newJoinBean.getLover());
            context.write(k,v);
        }
    }
}
