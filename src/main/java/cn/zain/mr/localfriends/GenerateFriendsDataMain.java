package cn.zain.mr.localfriends;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Created by yongz on 2018/2/23.
 */
public class GenerateFriendsDataMain {
    static String[] nameDic = {"寒锦","妍慧","琛雨","格枫","彩锦","媛彩","妍敏","婷雨",
            "馨柏","月欣","旭薇","璐初","冬弦","曦静","玉呈","初华","雯蔚","璐洁","格诗",
            "莲嘉","采橘","岚婧","梦彩","栀妍","碧瑶","舒慧","鹤心","雅倩","雪玉",
            "菡楠","鸿春","凌妍","雨婧","凡婧","美莲","颖萱","桂丽","彩彦","美茜",
            "嘉歆","函玉","珊芝","华彩","月婧","岚璐","洁璇","琪韵","婷雪","采雪",
            "雪月","薇柏","璟琳","雅玉","萱彤","妍怡","柔梅","鸿彩","锦楠","玥雨",
            "锦彩","彩欢","格洲","花慧","萱彩","萱璟","怡桐","橘帆","霞芳","莲梦",
            "彩曼","桃蓓","彩柏","玉琳","紫碧","珠晨","雪莲","雪初","枫杉","霞静","林雅"};
    private static Random random = new Random();

    public static void main(String[] args) {
        for (int i = 0; i < nameDic.length; i++) {
            generateOneFriends(nameDic[i]);
        }
    }

    private static void generateOneFriends(String one) {
        Set<String> friendSet = new HashSet<>();
        int num = random.nextInt(8)+1; //1-8个好友
        while (friendSet.size() < num){
            //随机取一个名字
            int order = random.nextInt(nameDic.length);
            String tmp = nameDic[order];
            if(!one.equals(tmp)){
                friendSet.add(tmp);
            }
        }
        StringBuffer sb = new StringBuffer(one+":");
        for (String friend:friendSet) {
            sb.append(friend).append(",");
        }

        System.out.println(sb.substring(0,sb.length()-1).toString());
    }
}
