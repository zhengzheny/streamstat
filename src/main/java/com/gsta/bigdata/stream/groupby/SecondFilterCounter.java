package com.gsta.bigdata.stream.groupby;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

//import net.sf.json.JSONObject;
import com.google.gson.Gson;
import com.gsta.bigdata.stream.BloomFilterFactory;
import com.gsta.bigdata.stream.CounterCacheSingleton;
import com.gsta.bigdata.stream.CounterCount;
import com.gsta.bigdata.stream.counter.CountTimeStamp;
import com.gsta.bigdata.stream.utils.Constants;

public class SecondFilterCounter {
	//key=计数器中的key,由timestamp+keyFields组成,GroupbyCount由kafka读取的map
	private Map<String, GroupbyCount> counters = new ConcurrentHashMap<String, GroupbyCount>();
	//key的创建时间,用于扫描进程清理计数器
	private Map<String, CountTimeStamp> countersTimeStamp = new ConcurrentHashMap<String, CountTimeStamp>();
	//counter计数器进程个数
	private int streamAgentCnt;
	private  String selectedFilter;
	private AtomicLong totalCount = new AtomicLong(1);
	private AtomicLong flushCount = new AtomicLong(0);
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	private final Gson gson = new Gson();
	private long repeatCount = 1;
	
	public SecondFilterCounter(int streamAgentCnt) {
		this.streamAgentCnt = streamAgentCnt;
	}

	public void groupby(String jsonMsg){
		if(jsonMsg == null)  return;	
		CounterCount counterCount= this.gson.fromJson(jsonMsg, CounterCount.class);
		if(counterCount != null){
			String key = counterCount.getValue(Constants.OUTPUT_FIELD_KEY);
			if(key == null) return;
//			截取key中的timestamp
			String keyfileds[] = key.split(Constants.KEY_DELIMITER);
			long timestamp = 0;
			try{
			 timestamp = Long.parseLong(keyfileds[1]);
			} catch(NumberFormatException e){
				logger.error("invalid timestamp:{}",e.getMessage()+ "key=" +key);
				return;
			}
			String counterName = counterCount.getValue(Constants.OUTPUT_FIELD_COUNTER_NAME);
			Map<String, String>valueData = counterCount.getMap();
			//key=counterName+key
			key =  counterName + Constants.KEY_DELIMITER + key;
			if (counterName.equals("cgicount-1hour") ){
				 selectedFilter = "second-CGI-bloomFilter";}
			else{  selectedFilter = "second-domain-bloomFilter";}
				boolean isExist = BloomFilterFactory.getInstance().isExist(
					selectedFilter, timestamp, valueData);
//				判断key值是否存在，如果不存在，创建counter
				if (!isExist){							
						counters.put(key, new GroupbyCount(counterCount));
						countersTimeStamp.computeIfAbsent(key, k->new CountTimeStamp());	
				}else{	
					    GroupbyCount groupbyCount = counters.get(key);
						if(groupbyCount != null){
							int cnt = groupbyCount.getCnt();
							if(cnt >= this.streamAgentCnt){
								CounterCacheSingleton.getSingleton().offer(groupbyCount);
								counters.remove(key);
								countersTimeStamp.remove(key);
								this.flushCount.incrementAndGet();
							}
								repeatCount++;	
							if(repeatCount % 10000 == 0){
			logger.info("{} has repeat count={}","second-CGI-bloomFilter",repeatCount);
							}}	
					}
						BloomFilterFactory.getInstance().add(timestamp, valueData);
				}
				this.totalCount.incrementAndGet();
			}

	public Map<String, GroupbyCount> getCounters() {
		return counters;
	}

	public Map<String, CountTimeStamp> getCountersTimeStamp() {
		return countersTimeStamp;
	}
}
