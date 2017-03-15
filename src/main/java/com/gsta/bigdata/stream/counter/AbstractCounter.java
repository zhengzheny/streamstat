package com.gsta.bigdata.stream.counter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.gsta.bigdata.stream.FlushesFactory;
import com.gsta.bigdata.stream.flush.IFlush;
import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;

public abstract class AbstractCounter {
	//存放计数器结果
	//key=timestamp or key=field+timestamp,field is mdn,domain,ip,cgi
	private Map<String, Count> counters = new ConcurrentHashMap<String, Count>();
	//存放计数器时间,只有创建时候才加入,在flush出去时,只需要扫描countersTimeStamp,减少对counters的操作,提高性能
	private Map<String, CountTimeStamp> countersTimeStamp = new ConcurrentHashMap<String, CountTimeStamp>();
	//name from configure file
	protected String name;
	private IFlush[] flushes;
	private IFlush[] continuousFlushes;
	private int flushWaitTime;
	private String bloomFilterName;
	private String[] keyFields;
	//写到kafka的topic
	private String outputTopic;
		
	public AbstractCounter(String name){
		this.name = name;
		
		Map<String, Object> configs = ConfigSingleton.getInstance().getCounterConf(name);
		if (configs != null) {
			//获取flush,不设置默认为console
			String flushes = (String)configs.get("flushes");
			String[] flushesName;
			if(flushes != null){
				flushesName = flushes.split(",",-1);
			}else{
				flushesName  = new String[1];
				flushesName[0] = Constants.FLUSH_CONSOLE;
			}
			if(flushesName != null){
				this.flushes = new IFlush[flushesName.length];
				for(int i=0;i<flushesName.length;i++){
					this.flushes[i] = FlushesFactory.createFlush(flushesName[i]);
				}
			}
				
			//获取continuousFlushes
			flushes = (String)configs.get("continuousFlushes");
			if(flushes != null){
				flushesName = flushes.split(",",-1);
				if (flushesName != null) {
					this.continuousFlushes = new IFlush[flushesName.length];
					for (int i = 0; i < flushesName.length; i++) {
						this.continuousFlushes[i] = FlushesFactory.createFlush(flushesName[i]);
					}
				}
			}
			
			this.flushWaitTime = (int)configs.getOrDefault("continuousFlushWaitTime", 60);
			this.bloomFilterName = (String)configs.get("bloomFilter");
			
			String strFields = (String)configs.get("keyFields");
			if(strFields != null){
				this.keyFields = strFields.split(",", -1);
			}
			
			//如果不设置输出的kafka topic,用kafkaCluster定义的默认output topic
			this.outputTopic = (String)configs.get("outputKafkaTopic");
			if(this.outputTopic == null){
				this.outputTopic = ConfigSingleton.getInstance().getDefaultKafkaOutputTopic();
			}
		}//end of if (configs != null)
	}

	public abstract void add(String kafkaKey, Map<String, String> valueData,
			String mdn, long timeStamp);

	public Map<String, Count> getCounters() {
		return counters;
	}

	public Map<String, CountTimeStamp> getCountersTimeStamp() {
		return countersTimeStamp;
	}
	
	/**
	 * 
	 * @param key - 计数器的key
	 * @param stepLen  - 计数器的步长,如流量类的累积
	 */
	public void addCount(String key,long stepLen){
		if(key == null) return;
		
		this.counters.computeIfAbsent(key, k -> new Count()).inc(stepLen);
	}
	
	public void addCount(String key){
		if(key == null) return;
		
		this.counters.computeIfAbsent(key, k -> new Count()).inc();
	}
	
	public void addCountTimeStamp(String key){
		if(key == null) return;
		
		//已经存在的计数器，不用更新计数器时间，以创建时间为主
		if(!this.countersTimeStamp.containsKey(key)){
			this.countersTimeStamp.put(key, new CountTimeStamp());
		}
	}
	
	/**
	 * 
	 * @param key
	 * @param timeStamp 对于以天为单位的，以系统的0点0分0秒为创建时间，过期时间为24小时
	 */
	public void addCountTimeStamp(String key,long timeStamp){
		if(key == null) return;
		
		//已经存在的计数器，不用更新计数器时间，以创建时间为主
		if(!this.countersTimeStamp.containsKey(key)){
			this.countersTimeStamp.put(key, new CountTimeStamp(timeStamp));
		}
	}

	public String getName() {
		return this.name;
	}
	
	//millis
	public abstract long getFlushTimeGap();
	
	public IFlush[] getFlushes() {
		return this.flushes;
	}
	
	public IFlush[] getContinuousFlushes(){
		return this.continuousFlushes;
	}

	//second
	public int getFlushWaitTime() {
		return flushWaitTime;
	}
	
	public String getBloomFilterName() {
		return bloomFilterName;
	}

	public String[] getKeyFields() {
		return keyFields;
	}

	public String getOutputTopic() {
		return outputTopic;
	}

	public static String parseMdnPrefix(String mdn, boolean shortmdnprefix) {
		if (mdn == null) return null;
		if (mdn.length() <= 4) return mdn;

		//在计数器中设置了shortmdnprefix: true,表示取前7位,否则取前3位
		if (shortmdnprefix) {
			return mdn.substring(0, mdn.length() - 8);
		} else {
			return mdn.substring(0, mdn.length() - 4);
		}
	}
}
