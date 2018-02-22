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