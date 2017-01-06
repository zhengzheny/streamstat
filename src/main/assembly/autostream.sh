count=`/home/noce/jdk1.8.0_45/bin/jps -l | grep App | wc -l`
curr=`date +"%Y-%m-%d %H:%M:%S"`
if [ $count -lt 10 ]
then
    nohup /home/noce/kafkastream/bin/etl-kafkastream.sh  &
    echo $curr" start stream agent..."
else
    echo $curr" no start stream agent..."
fi
