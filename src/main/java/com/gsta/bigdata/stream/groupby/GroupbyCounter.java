package com.gsta.bigdata.stream.groupby;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;





//import net.sf.json.JSONObject;
import com.google.gson.Gson;
import com.gsta.bigdata.stream.CounterCacheSingleton;
import com.gsta.bigdata.stream.CounterCount;
import com.gsta.bigdata.stream.counter.CountTimeStamp;
import com.gsta.bigdata.stream.utils.Constants;

public class GroupbyCounter {
	//key=计数器中的key,由timestamp+keyFields组成,GroupbyCount由kafka读取的map
	private Map<String, GroupbyCount> counters = new ConcurrentHashMap<String, GroupbyCount>();
	//key的创建时间,用于扫描进程清理计数器
	private Map<String, CountTimeStamp> countersTimeStamp = new ConcurrentHashMap<String, CountTimeStamp>();
	//counter计数器进程个数
	private int streamAgentCnt;
	private AtomicLong totalCount = new AtomicLong(1);
	private AtomicLong flushCount = new AtomicLong(0);
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	private final Gson gson = new Gson();
	
	public GroupbyCounter(int streamAgentCnt) {
		this.streamAgentCnt = streamAgentCnt;
	}

	public void groupby(String jsonMsg){
		if(jsonMsg == null)  return;
		
		CounterCount counterCount = this.gson.fromJson(jsonMsg, CounterCount.class);
		if(counterCount != null){
			//key=counterName+key
			String key = counterCount.getValue(Constants.OUTPUT_FIELD_KEY);
			if(key == null) return;
			key = counterCount.getValue(Constants.OUTPUT_FIELD_COUNTER_NAME) + Constants.KEY_DELIMITER + key;
			if(counters.containsKey(key)){
				//如果已经存在，累积count值,并判断是否已经等到了所有进程的计数器,如果等齐了,发送出去,并从计数器中删除
				GroupbyCount groupbyCount = counters.get(key);
				if(groupbyCount != null){
					groupbyCount.groupby(counterCount);
					int cnt = groupbyCount.getCnt();
					if(cnt >= this.streamAgentCnt){
						CounterCacheSingleton.getSingleton().offer(groupbyCount);
						counters.remove(key);
						countersTimeStamp.remove(key);
						this.flushCount.incrementAndGet();
					}
				}
			}else{
				counters.put(key, new GroupbyCount(counterCount));
				countersTimeStamp.computeIfAbsent(key, k->new CountTimeStamp());
			}
			
			this.totalCount.incrementAndGet();
		}
		
		if(this.totalCount.get() % 10000 == 0){
			logger.info("total count={},flush all stream agent count={}",this.totalCount.get(),this.flushCount.get());
		}
	}
	
	public Map<String, GroupbyCount> getCounters() {
		return counters;
	}

	public Map<String, CountTimeStamp> getCountersTimeStamp() {
		return countersTimeStamp;
	}
}
