PIDS=$(/home/noce/jdk1.8.0_45/bin/jps -l | grep Application | awk -F' ' '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No kafka stream application to stop"
  exit 1
else
  kill -s TERM $PIDS
fi
