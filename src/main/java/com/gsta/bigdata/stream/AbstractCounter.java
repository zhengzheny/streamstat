package com.gsta.bigdata.stream;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.gsta.bigdata.stream.utils.ConfigSingleton;

public abstract class AbstractCounter {
	//default-ConcurrentHashMap
	//key=timestamp or key=field+timestamp,field is mdn,domain,ip,cgi
	private Map<String, Count> counters = new ConcurrentHashMap<String, Count>();
	//name from configure file
	protected String name;
	private IFlush[] flushes;
	private IFlush[] continuousFlushes;
	private int flushWaitTime;
	private String bloomFilterName;
	private String[] keyFields;
		
	public AbstractCounter(String name){
		this.name = name;
		
		//get flushes
		String[] flushesName = ConfigSingleton.getInstance().getCounterFlushes(name);
		this.flushes = new IFlush[flushesName.length];
		for(int i=0;i<flushesName.length;i++){
			this.flushes[i] = FlushesFactory.createFlush(flushesName[i]);
		}
		
		//get continuous flush,don't remove result from memory
		flushesName = ConfigSingleton.getInstance().getCounterContinuousFlushes(name);
		if (flushesName != null) {
			this.continuousFlushes = new IFlush[flushesName.length];
			for (int i = 0; i < flushesName.length; i++) {
				this.continuousFlushes[i] = FlushesFactory.createFlush(flushesName[i]);
			}
		}
		
		this.flushWaitTime = ConfigSingleton.getInstance().getCounterFlushWaitTime(name);
		this.bloomFilterName = ConfigSingleton.getInstance().getCounterBloomFilter(name);
		
		String strFields = ConfigSingleton.getInstance().getCounterStatField(name);
		if(strFields != null){
			this.keyFields = strFields.split(",", -1);
		}
	}

	public abstract void add(String kafkaKey, Map<String, String> valueData,
			String mdn, long timeStamp);

	public Map<String, Count> getCounters() {
		return counters;
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

	public static String parseMdnPrefix(String mdn, boolean shortmdnprefix) {
		if (mdn == null) return null;
		if (mdn.length() <= 4) return mdn;

		// if short mdn prefix
		if (shortmdnprefix) {
			return mdn.substring(0, mdn.length() - 8);
		} else {
			return mdn.substring(0, mdn.length() - 4);
		}
	}
}
