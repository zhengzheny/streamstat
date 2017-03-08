if [ $# -lt 2 ]
then
  echo "usage:bin/startStream.sh configFile agentCount"
  exit -1
fi

streamAgentCount=$2
configFile=$1
configPath=./conf/instance

if [ ! -d "$configPath" ]
then
  mkdir -p $configPath
else
  rm -f $configPath/*
fi

count=`jps -l | grep App | wc -l`
if [ $count -lt $streamAgentCount ]
then
  for((i=1;i<=$streamAgentCount;i++))
    do
    config=$configPath/config$i.yaml
    path="/data/kafkastream/state/config$i/"
        sed "s:STATE_DIR:${path}:g" $configFile > $config
        nohup bin/etl-kafkastream.sh $config $i &
        echo "start $i  stream agent..."
    done
fi

