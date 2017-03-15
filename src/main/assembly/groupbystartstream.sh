if [ $# -lt 5 ]
then
  echo "usage:bin/groupbystartStream.sh configFile inputTopic outputTopic streamAgentCount jmxport"
  exit -1
fi

curr=`date +"%Y%m%d"`
configFile=$1
inputTopic=$2 
outputTopic=$3
applicationId="groupby-$inputTopic-$curr"
counterstreamAgentNum=40
#unit is second
flushTime=300
streamAgentCount=$4
jmxport=$5
configPath=./conf/instance/groupby/$inputTopic

if [ ! -d "$configPath" ]
then
  mkdir -p $configPath
else
  rm -f $configPath/*
fi

for((i=1;i<=$streamAgentCount;i++))
do
    config=$configPath/config$i.yaml
    path="/data/kafkastream/groupby/$inputTopic/state/config$i/"
    sed "s:STATE_DIR:${path}:g" $configFile > $config
    nohup bin/groupbyetl-kafkastream.sh $config $applicationId $inputTopic $outputTopic $counterstreamAgentNum $flushTime  $i $jmxport &
    echo "start $i  stream agent..."
done

