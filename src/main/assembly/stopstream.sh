jps -l | grep com.gsta.bigdata.stream.Application  | awk -F' ' '{print $1}' | xargs kill 
