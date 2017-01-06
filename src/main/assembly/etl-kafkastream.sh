#!/bin/sh

BASEDIR=`dirname "$0"`/..
cd $BASEDIR
  
BIGDATA_CLASSPATH="$BASEDIR/conf:$BASEDIR/lib/"
for i in "$BASEDIR"/lib/*.jar
do
  BIGDATA_CLASSPATH="$BIGDATA_CLASSPATH:$i"
done

#RUN_CMD="\"$JAVA_HOME/bin/java\""
RUN_CMD="java "
RUN_CMD="$RUN_CMD -classpath \"$BIGDATA_CLASSPATH\""
RUN_CMD="$RUN_CMD -Xmx4G -Xms4G "
RUN_CMD="$RUN_CMD com.gsta.bigdata.stream.Application $@"
  
echo $RUN_CMD
eval $RUN_CMD