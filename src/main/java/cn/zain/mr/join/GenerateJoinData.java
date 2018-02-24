package cn.zain.mr.join;

import cn.zain.mr.StringTools;

import java.util.HashSet;
import java.util.Random;

/**
 * Created by yongz on 2018/2/24.
 */
public class GenerateJoinData {
    static Random random = new Random();
    static HashSet<String> users = new HashSet<>();
    static HashSet<String> lovers = new HashSet<>();
    public static void main(String[] args) {
        for (int i = 0; i < 100; i++) {
//            generateOrderData(i);
            generateUserData(i);
        }
    }



    private static void generateOrderData(int i) {
        StringBuilder sb = new StringBuilder("O");
        sb.append(10000+i);
        sb.append(" u").append(1000+random.nextInt(99)+1);
        System.out.println(sb);
    }

    private static void generateUserData(int i) {
        String user = StringTools.genRandomStr(random.nextInt(5) + 1);
        String lover = StringTools.genRandomStr(random.nextInt(5) + 1);
        if(users.contains(user) ||lovers.contains(lover)){
            generateUserData(i);
        }
        users.add(user);
        lovers.add(lover);

        StringBuilder sb = new StringBuilder("u");
        sb.append(1000+i);
        sb.append(" ").append(user).append(" ").append(StringTools.genRandomNum(2)).append(" ").append(lover);
        System.out.println(sb);
    }
}
