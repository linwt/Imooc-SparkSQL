# ImoocSparkSQLProject
慕课网日志分析项目
## 一、使用步骤
1. 将项目克隆到本地
2. 将Imooc_SparkSQL和Imooc_Web项目分别导入IDEA
3. 正确设置Imooc_Data里日志文件的路径
4. 阅读代码，运行项目
## 二、软件版本
1. spark-2.1.0-bin-hadoop2.6
2. scala-2.11.8
3. hadoop-2.6.4
## 三、项目需求
1. 按天统计最受欢迎课程
2. 按城市统计最受欢迎课程
3. 按流量统计最受欢迎课程
## 四、项目思路
### 1. Format一次清洗
1. 读取初始日志
2. 进行数据一次清洗
> - **输入格式**<br>
218.75.35.226 - - [11/05/2017:08:07:35 +0800] "POST /api3/getadv HTTP/1.1" 200 407 "http://www.imooc.com/article/17891" "-" cid=0&timestamp=1455254555&uid=5844555<br>
> - **输出格式：“date url traffic ip”**<br>
2017-05-11 08:07:35	http://www.imooc.com/article/17891	407	218.75.35.226
3. 保存结果
### 2. Clean二次清洗
1. 读取一次清洗结果文件
2. 进行数据二次清洗
> - **输入格式：“date url traffic ip”**<br>
2017-05-11 08:07:35	http://www.imooc.com/article/17891	407	218.75.35.226<br>
> - **输出格式：“url courseType courseId traffic ip city time day”**<br>
http://www.imooc.com/article/17891  article  17891  407  218.75.35.226  北京  08:07:35  2017-05-11
3. 创建DataFrame
4. 保存结果为parquet文件类型
### 3. TopN统计分析
1. 读取二次清洗结果文件
2. 使用DataFrame方式或MySQL方式统计数据
3. dos中启动mysql，创建数据库和相应表
4. 调用DAO将结果写入MySQL数据库
### 4. Spark on YARN
#### 1） CleanYarn数据清洗运行在yarn上
1. 打包
> - File → Project Structure → Artifacts → “+” → JAR → From modules with dependencies → Main Class:CleanYarn → OK
> - Build → Build Artifacts → Build 
2. 上传"Imooc_SparkSQL.jar、ipDatabase.csv、ipResgion.xlsx、format日志文件"到linux的/home/hadoop/imooc/
3. 启动hadoop，上传format日志文件到hdfs的/imooc/input/
4. 导入hadoop路径
> - 方式一：执行命令export HADOOP_CONF_DIR=/home/hadoop/apps/hadoop/etc/hadoop
> - 方式二：spark/conf/spark-env.sh配置文件中添加：export YARN_CONF_DIR=/home/hadoop/apps/hadoop/etc/hadoop
5. 提交作业
> [hadoop@mini1 spark]$ bin/spark-submit \
> --class main.CleanYarn \
> --name CleanYarn \
> --master yarn \
> --executor-memory 1G \
> --num-executors 1 \
> --files /home/hadoop/imooc/ipDatabase.csv,/home/hadoop/imooc/ipRegion.xlsx \
> /home/hadoop/imooc/Imooc_SparkSQL.jar \
> hdfs://mini1:9000/imooc/input/* \
> hdfs://mini1:9000/imooc/clean
6. hdfs查看运行结果，即产生clean目录文件

#### 2） TopNYarn统计分析运行在yarn上
1. 打包
> - Build → Build Artifacts → Edit → Main Class:TopNYarn → OK
> - Build → Build Artifacts → Rebuild
2. 删除旧jar包，重新上传新jar包到linux的/home/hadoop/imooc/
3. linux中启动mysql，创建数据库和相应表
4. 提交作业
> [hadoop@mini1 spark]$ bin/spark-submit \
> --class main.TopNYarn \
> --name TopNYarn \
> --master yarn \
> --executor-memory 1G \
> --num-executors 1 \
> /home/hadoop/imooc/Imooc_SparkSQL.jar \
> hdfs://mini1:9000/imooc/clean 2017-05-11
5. linux的mysql中查看运行结果，即插入数据到表
#### 3） web访问路径
> - yarn   mini1:8088
> - hdfs   mini1:50070

### 5. 可视化展示
1. 配置、启动tomcat
2. 读取mysql数据，使用Echarts展示
3. 浏览器访问：http://localhost:8080/Imooc_Web/topn.html
4. 展示效果<br>
![](https://github.com/linwt/ImoocSparkSQLProject/blob/master/Imooc_Data/imooc.png)
## 五、其他
### 1. 使用github开源项目ipDatabase（由ip获取城市）
1. https://github.com/wzhe06/ipdatabase <br>
fork到自己仓库，克隆项目到本地（D:\）
2. 编译项目：D:\ipdatabase> mvn clean package –DskipTests <br>
3. 安装jar包到自己的maven仓库
> D:\ipdatabase>mvn install:install-file \
-Dfile=D:\ipdatabase\target\ipdatabase-1.0-SNAPSHOT.jar \
-DgroupId=com.ggstar \
-DartifactId=ipdatabase \
-Dversion=1.0 \
-Dpackaging=jar
4. 导入依赖

> <dependency>
    <groupId>com.ggstar</groupId>
    <artifactId>ipdatabase</artifactId>
    <version>1.0</version>
</dependency>

> <dependency>
    <groupId>org.apache.poi</groupId>
    <artifactId>poi-ooxml</artifactId>
    <version>3.14</version>
</dependency>

> <dependency>
    <groupId>org.apache.poi</groupId>
    <artifactId>poi</artifactId>
    <version>3.14</version>
</dependency>

5. 将ipDatabase/target/classes/ipDatabase.csv和ipDatabase/target/classes/ipRegion.xlsx复制到src/main/resources/

### 2. 数据库相关代码
1. 创建数据库 <br>
> mysql> create database imooc;
2. 创建表
- 按天统计
> mysql> create table day_top ( \
day varchar(10) not null, \
courseId bigint(10) not null, \
times bigint(10) not null, \
primary key (day,courseId) \
);
- 按城市统计
> mysql> create table city_top( \
day varchar(10) not null, \
courseId bigint(10) not null, \
city varchar(10) not null, \
times bigint(10) not null, \
timesRank int not null, \
primary key (day,courseId,city) \
);
- 按流量统计
> mysql> create table traffic_top( \
day varchar(10) not null, \
courseId bigint(10) not null, \
traffics bigint(10) not null, \
primary key (day, courseId) \
);





