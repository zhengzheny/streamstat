if [ $# -lt 5 ]
then
  echo "usage:bin/secondfilterstartStream.sh configFile  inputTopic outputTopic streamAgentCount jmxport"
  exit -1
fi

curr=`date +"%Y%m%d"`
configFile=$1
inputTopic=$2 
outputTopic=$3
#applicationId="groupby-$inputTopic-$curr"
applicationId="secondfilter-$inputTopic-20170315"
counterstreamAgentNum=40
#unit is second
flushTime=300
initbloomFilters="second-CGI-bloomFilter,second-domain-bloomFilter"
streamAgentCount=$4
jmxport=$5
configPath=./conf/instance/secondfilter/$inputTopic

if [ ! -d "$configPath" ]
then
  mkdir -p $configPath
else
  rm -f $configPath/*
fi

for((i=1;i<=$streamAgentCount;i++))
do
    config=$configPath/config$i.yaml
    path="/data/kafkastream/secondfilter/$inputTopic/state/config$i/"
    sed "s:STATE_DIR:${path}:g" $configFile > $config
    nohup bin/secondfilteretl-kafkastream.sh $config $applicationId $initbloomFilters $inputTopic $outputTopic $counterstreamAgentNum $flushTime  $i $jmxport &
    echo "start $i  stream agent..."
done

