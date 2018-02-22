# hadoop_mapreduce
map+reduce程序

mvn package assembly:single
  或mvn assembly:assembly

建议用hadoop jar xx.jar自动设置classpath

cn.zain.mr.count
    用于简单计数功能,按空格截断统计即可
    日志格式：
    ```Fifty years have now elapsed since Auguste Comtes monumental work, 
         the Cours de Philosophie Positive, was first introduced to English readers by Miss Harriet Martineau.
         SOME unpoetic old frontiersman first called the place a trappers hole,an ugly, 
         misleading name for this wondrous mountain valley, 
         lying up there on the western slopes of the Continental Divide next to the Yellowstone country
     pom中修改main为cn.zain.mr.count
cn.zain.mr.flow
    用于流量统计
    格式 ip  上行 下行  状态
        192.168.12.22  120 23 200
    pom中修改main为cn.zain.mr.flow

a.txt   
    hello tom
    hello jim
    hello kitty
b.txt
    java scala
    java python c++
c.txt
    hello tom
    hello kitty
统计 hello a.txt>3 c.txt>2
    tom a.txt>1 c.txt>1
    ....
思路：
    以《单词》+《文件名》作为key value：1
    reduce 《单词》+《文件名》：1
    结果：hello-a.txt 3
         hello-c.txt  2
    
    map
    hello :a.txt 3
    hello :c.txt 3
    
    reduce
    hadoop jar indexStepOne.jar 3 /data/index/input /data/index/output1
    hadoop jar IndexStepTwo.jar 1 /data/index/output1 /data/index/output2