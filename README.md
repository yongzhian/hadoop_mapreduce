# hadoop_mapreduce

#### map+reduce程序

mvn package assembly:single
  或mvn assembly:assembly

建议用hadoop jar xx.jar自动设置classpath

#### 各个项目
#####cn.zain.mr.count
    用于简单计数功能,按空格截断统计即可
    日志格式：
    ```Fifty years have now elapsed since Auguste Comtes monumental work, 
         the Cours de Philosophie Positive, was first introduced to English readers by Miss Harriet Martineau.
         SOME unpoetic old frontiersman first called the place a trappers hole,an ugly, 
         misleading name for this wondrous mountain valley, 
         lying up there on the western slopes of the Continental Divide next to the Yellowstone country
     pom中修改main为cn.zain.mr.count
#####cn.zain.mr.flow
    用于流量统计
    格式 ip  上行 下行  状态
        192.168.12.22  120 23 200
    pom中修改main为cn.zain.mr.flow

#####cn.zain.mr.index 
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

#####cn.zain.mr.localcount

本地单词统计
1 D:\software_dev\JetBrains\IDEA2016.1.3_workspace_git\mapreduce\doc\localcount\input D:\software_dev\JetBrains\IDEA2016.1.3_workspace_git\mapreduce\doc\localcount\out
需要修改win
D:\software_dev\JetBrains\IDEA2016.1.3_workspace_git\mapreduce\doc\localcount\hadoop2.9.0 win32 64.rar
1：将文件解压到hadoop的bin目录下
2：将hadoop.dll复制到C:\Window\System32下
3：添加环境变量HADOOP_HOME，指向hadoop目录
4：将%HADOOP_HOME%\bin加入到path里面
5：重启myeclipse或者eclipse

#####cn.zain.mr.localuserrating
    只返回topn，内部传入参数