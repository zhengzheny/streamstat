if [ $# -lt 2 ]
then
  echo "usage:bin/startStream.sh configFile agentCount"
  exit -1
fi

streamAgentCount=$2
configFile=$1
configPath=./conf/instance/counter

curr=`date +"%Y%m%d"`
applicationId="counter-$curr"

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
        nohup bin/etl-kafkastream.sh $config $applicationId $i &
        echo "start $i  stream agent..."
    done
fi

