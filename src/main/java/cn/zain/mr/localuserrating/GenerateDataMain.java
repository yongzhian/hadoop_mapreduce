package cn.zain.mr.localuserrating;

import java.util.Random;

/**
 * Created by yongz on 2018/2/23.
 */
public class GenerateDataMain {
    private static Random random = new Random();
    public static void main(String[] args) {
        System.out.println("generateSingleData...");
        for (int i = 0; i < 5000 ; i++) {
            generateSingleData();
        }
        System.out.println("generateSingleData finished.");
    }

    private static void generateSingleData() {
        String str = "{\"movie\":\"A\",\"rate\":\"B\",\"timestamp\":\"C\",\"uid\":\"D\"}";
        int movie = random.nextInt(899) + 100;
        int rate = random.nextInt(10) + 1;
        long timestamp = 1519384600000L +  random.nextInt(89999) + 10000;
        int uid = random.nextInt(899) + 1000;
        str = str.replaceFirst("A",String.valueOf(movie))
                .replaceFirst("B",String.valueOf(rate))
                .replaceFirst("C",String.valueOf(timestamp))
                .replaceFirst("D",String.valueOf(uid));
        System.out.println(str);
    }
}
