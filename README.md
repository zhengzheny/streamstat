# streamstat

从kafka读取数据，并进行计数，把计数结果写入到kafka重新分区，基于新分区进行groupby操作，并把结果再次写入到kafka，通过logstash把最终统计结果写入到搜索引擎中。

* <font face="黑体">步骤</font>
* 1.maven编译，产生streamstat-1.0-kafkastream.tar，并部署到5台机器上
* 2.部署kafka，每台机器两个broker

./installkafka.sh

* 3.创建kafka分区
分区名                     分区数
4GDPI        40

tempCGI      20

tempMain     20

CGICounter   20

mainCounter  20

* 4.启动counter计数器，5台机器，每台8个进程

./startstream.sh ./conf/config.yaml 8

* 5.启动groupby进程，5台机器，每台4个进程

./groupbystartstream.sh ./conf/config.yaml tempCGI CGICounter 4  29877

./groupbystartstream.sh ./conf/config.yaml tempMain mainCounter 4 39877
