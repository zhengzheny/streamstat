#!/bin/bash

logDirs=(/data/kafkastream/counter/logs/
/data/kafkastream/groupby/tempCGI/logs/
/data/kafkastream/groupby/tempMain/logs/
/data/kafkalogs/
)

for dir in ${logDirs[@]}
do
  /home/noce/installMedia/deleteLog.sh  $dir  
done
