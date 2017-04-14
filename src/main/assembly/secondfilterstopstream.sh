jps -l | grep com.gsta.bigdata.stream.SecondBloomFilterApp  | awk -F' ' '{print $1}' | xargs kill 
