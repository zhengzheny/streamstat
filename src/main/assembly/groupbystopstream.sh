jps -l | grep com.gsta.bigdata.stream.GroupbyCounterApp  | awk -F' ' '{print $1}' | xargs kill 
