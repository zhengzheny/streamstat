configPath="./config/instance"
brokerCount=2

ip=`ifconfig bond0 |grep "inet addr" | awk '{print $2}' | awk -F: '{print $2}' | awk -F. '{print $NF}'`

if [ ! -d "$configPath" ]
then
  mkdir -p $configPath
else
  rm -f $configPath/*
fi

((p=14/$brokerCount))
for((i=1;i<=$brokerCount;i++))
do
  brokerid="$ip$i"
  ((brokerport=9090+i))
  
  file="$configPath/server$brokerid.properties"
  echo "broker.id=$brokerid" > $file
  echo "auto.create.topics.enable=false" >> $file
  echo "delete.topic.enable=true" >> $file
  echo "listeners=PLAINTEXT://:$brokerport" >> $file
  echo "num.network.threads=9" >> $file
  echo "num.io.threads=16" >> $file
  echo "socket.send.buffer.bytes=102400" >> $file
  echo "socket.receive.buffer..bytes=102400" >> $file
  echo "socket.request.max.bytes=104857600" >> $file
  echo "log.dirs=/data/kafkadata/kafka-logs$i" >> $file
  echo "num.partitions=1" >> $file
  echo "num.recovery.threads.per.data.dir=1" >> $file
  echo "log.retention.hours=12" >> $file
  echo "log.segment.bytes=1073741824" >> $file
  echo "log.retention.check.interval.ms=300000" >> $file
  echo "zookeeper.connect=XTH04-Sugoni840-10:2182,XTH04-Sugoni840-11:2182,XTH04-Sugoni840-12:2182/kafkaCluster" >> $file
  echo "zookeeper.connection.timeout.ms=6000" >> $file
  echo "reserved.broker.max.id=2000" >> $file
  echo "auto.leader.rebalance.enable=true" >> $file
 
  ((port=19090+$i))
  JMX_PORT=$port bin/kafka-server-start.sh -daemon $file 
  echo "start broker $i"
done