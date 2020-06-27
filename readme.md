# 在线教育项目

## 一、项目架构

#### hadoop + flume + kafka + canal + hive + reids = hbase + elastic search + spark sql + spark Streaming + mysql

## 二、整体设计

#### 数仓分层

##### DWD ：将原始数据进行ETL，维度退化，脱敏，字段解析，数仓建模（选择业务过程->声明粒度->确定维度->确定事实）

##### DWS：对DWD层的数据进行维度聚合，形成宽表

##### ADS：对宽表字段进行需求分析，指标统计

## 三、项目总结

##### 1.flume   hdfs sink导数据  需要控制回滚时间 回滚大小以及event个数的参数设置，防止产生小文件

##### 2.ods的数据是行式存储 +压缩 + 分区表

##### 3.windows机器要去访问集群资源，需要导入4个配置配置文件，分别是hive-stie.xml,hdfs-site.xml,yarn-site.xml,core-siet.xml

##### 4.保证ha,可以访问集群

`ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")`
`ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")`

##### 5.动态分区  静态分区的区别

##### 开启动态分区条件：

`spark.sql("set hive.exec.dynamic.partition=true")`
`spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")`

##### 6.解析完数据，具体返回值是tuple元组 还是case class由表的字段数决定，小于22个 可以用tuple元组，如果大于22个必须用case class

##### 7.导入hive当中，缩小分区，目的解决小文件过多的问题   coalesce 算子。

##### 8.write.mode(SaveMode.Overwrite).insertInto 插入hive表

##### 9.insertinto  与saveastable,insertino兼容hive根据顺序去匹配       saveastable根据列名去匹配不兼容hive

##### 10.df 之间的join  (1)===   (2)seq  两种方式，两种3个===不会去重join列，seq会去重join列

##### 11.groupbykey  可以自定义key, 然后根据key聚合形成的结构     （key,Iterator(case class .....)）

##### 12.对整一组进行（key,Iterator(case class .....)）一个map处理，那么就是mapgroups进行一个重组

##### 13.df ds 的cache缓存级别跟rdd是不一样的，默认采用使用磁盘跟内存

##### 14.df ds不存在reducebykey的，那么聚合数据必须groupby key，针对（key,Iterator(case class .....)）迭代器里每一个case class进行一个重组，那么可以用mapvalues算子对case class进行map转换
##### 15.import org.apache.spark.sql.functions._  这个包如果是spark做数仓，这个包必用，所有的函数都在里面

##### 16.广播join,只适用于小表join大表，只能广播小表  spark.sql.autoBroadcastJoinThreshold  默认值10mb

##### 17.spark sql默认的shuffle 分区个数是200，spark.sql.shuffle.partitions。凡是经过join,groupby 这些操作分区数都会变成默认值。分区数最好是 申请资源的cpu个数的2倍到3倍

##### 18.三种序列化   （1）java spark默认的   （2）kryo 体积更小，速度更快。需要注册  （3） df ds特有编码序列化 内存优化  如果是rdd  kryo序列化可以结合ser序列化缓存使用 df ds 差别不大 最优效果

##### 19.解决数据倾斜 

##### （1）打散大表，扩容小表的一个方案，那么这个方案 实验的结果：虽然解决数据倾斜但是时间更久了，原因就是小表扩容了数据量变大了

##### （2）广播join  也可以解决数据倾斜的问题

##### 20.SMB JOIN  优化大表join 大表 ，需要分桶，  两张表的桶的个数必须相等  分桶列==排序列==join列

