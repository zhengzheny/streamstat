count=`/home/noce/jdk1.8.0_45/bin/jps -l | grep App | wc -l`
if [ $count -lt 10 ]
then
    for((i=1;i<=10;i++))
    do
        nohup bin/etl-kafkastream.sh  &
        echo "start $i  stream agent..."
    done
fi
