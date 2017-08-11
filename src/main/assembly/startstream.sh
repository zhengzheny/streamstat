if [ $# -lt 2 ]
then
  echo "usage:bin/startStream.sh configFile agentCount"
  exit -1
fi

streamAgentCount=$2
configFile=$1
configPath=./conf/instance/counter

curr=`date +"%Y%m%d"`
#applicationId="counter-$curr"
applicationId="counter-20170315"
#初始化布隆过滤器列表
initbloomFilters="1hourSportCenter-mdn-bloomFilter,5minSportCenter-mdn-bloomFilter,1hourGsta-mdn-bloomFilter,5minGsta-mdn-bloomFilter,5min-mdn-bloomFilter,1hour-mdn-bloomFilter,1day-mdn-bloomFilter,1hour-CGI-bloomFilter,1hour-domain-bloomFilter,1hourTrain-mdn-bloomFilter,1hourSouthTrain-mdn-bloomFilter,1hourPhoneExpo-mdn-bloomFilter,1hourPhoneExpoHotel-mdn-bloomFilter,1hourPhoneExpoAirport-mdn-bloomFilter,5minTrain-mdn-bloomFilter,5minSouthTrain-mdn-bloomFilter,5minPhoneExpo-mdn-bloomFilter,5minPhoneExpoHotel-mdn-bloomFilter,5minPhoneExpoAirport-mdn-bloomFilter"

if [ ! -d "$configPath" ]
then
  mkdir -p $configPath
else
  rm -f $configPath/*
fi

count=`jps -l | grep com.gsta.bigdata.stream.Application | wc -l`
if [ $count -lt $streamAgentCount ]
then
  ((c=$streamAgentCount-$count))
  for((i=1;i<=$c;i++))
    do
        config=$configPath/config$i.yaml
        path="/data/kafkastream/counter/state/config$i/"
        sed "s:STATE_DIR:${path}:g" $configFile > $config
        nohup bin/etl-kafkastream.sh $config $applicationId $initbloomFilters $i &
        echo "start $i  stream agent..."
    done
fi

