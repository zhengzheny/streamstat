package com.gsta.bigdata.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.gsta.bigdata.stream.utils.ConfigSingleton;

public abstract class AbstractCounter {
	//default-ConcurrentHashMap
	//key=timestamp or key=field+timestamp,field is mdn,domain,ip,cgi
	private Map<String,Map<String, Count>> counters ;
	//name from configure file
	protected String name;
	private IFlush[] flushes;
	private IFlush[] continuousFlushes;
	private int flushWaitTime;
	protected final static String DEFALUT_COUNTER = "default";
	protected final static String MDN_PREFIX_COUNTER = "mdnPrefix";
		
	public AbstractCounter(String name){
		this.name = name;
		this.counters = new HashMap<String,Map<String,Count>>();
		this.counters.put(DEFALUT_COUNTER, new ConcurrentHashMap<String, Count>());
		
		//get flushes
		String[] flushesName = ConfigSingleton.getInstance().getCounterFlushes(name);
		this.flushes = new IFlush[flushesName.length];
		for(int i=0;i<flushesName.length;i++){
			this.flushes[i] = FlushesFactory.createFlush(flushesName[i]);
		}
		
		//get continuous flush,don't remove from counter
		flushesName = ConfigSingleton.getInstance().getCounterContinuousFlushes(name);
		if (flushesName != null) {
			this.continuousFlushes = new IFlush[flushesName.length];
			for (int i = 0; i < flushesName.length; i++) {
				this.continuousFlushes[i] = FlushesFactory.createFlush(flushesName[i]);
			}
		}
		
		this.flushWaitTime = ConfigSingleton.getInstance().getCounterFlushWaitTime(name);
	}
	
	public abstract void add(String kafkaKey,Map<String, String> valueData);
	
	public Map<String, Map<String, Count>> getCounters() {
		return counters;
	}
	
	public Map<String,Count> getDefaultCounter(){
		return this.counters.get(DEFALUT_COUNTER);
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
	
	public static String parseMdnPrefix(String mdn){
		if(mdn != null){
			if(mdn.length() <= 4) return mdn;
			return mdn.substring(0,mdn.length() - 4);
		}
		return null;
	}
}
