jps -l | grep App  | awk -F' ' '{print $1}' | xargs kill -9
