#!/bin/bash

if [ $# -lt 1 ] 
then
   echo "usage:deleteLog.sh logDir"
   exit -1
fi
    
expried_time=7    

currentDate=`date +%s`
echo "current date: " $currentDate

for file in `find $1 -name "*.log.*"` 
do
   name=$file
   modifyDate=$(stat -c %Y $file)

   logExistTime=$(($currentDate - $modifyDate))
   logExistTime=$(($logExistTime/86400))
       
   if [ $logExistTime -gt $expried_time ]; then
       echo "File: " $name "Modify Date: " $modifyDate + "Exist Time: " $logExistTime + "Delete: yes"
       rm -f $file
   else
       echo "File: " $name "Modify Date: " $modifyDate + "Exist Time: " $logExistTime + "Delete: no"
   fi
done
