package cn.zain.mr.efficientuserrating;

import java.util.HashMap;

/**
 * Created by yongz on 2018/2/26.
 */
public class DBLoader {

    public static void load(HashMap<String,String> map){
        //可通过jdbc给map赋值
        map.put("10","fullScore");
    }
}
