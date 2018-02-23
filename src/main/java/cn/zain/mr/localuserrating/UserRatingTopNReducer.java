package cn.zain.mr.localuserrating;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by yongz on 2018/2/23.
 */
public class UserRatingTopNReducer extends Reducer<Text,RateBean,Text,RateBean> {
    @Override
    protected void reduce(Text key, Iterable<RateBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<RateBean> list = new ArrayList<>();
        for (RateBean rateBean:values){
            //rateBean迭代出来始终是同一个对象，第一次加入后，后面之后修改加入对象的属性而已，故需要new一个bean
            RateBean rateBean2= new RateBean();
            try {
                BeanUtils.copyProperties(rateBean2,rateBean);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            list.add(rateBean2);
        }
        //本用户的评分数按大小排序
        Collections.sort(list);

        int N =  context.getConfiguration().getInt("user.rate.topn",5);
        if(list.size()< N){
            N= list.size();
        }
        //前N个
        for (int i = 0; i < N; i++) {
            context.write(key,list.get(i));
        }

    }
}
