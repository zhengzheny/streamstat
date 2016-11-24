package com.gsta.bigdata.stream;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.WindowTime;

public class DPIUserStatCounter extends AbstractCounter {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	//time gap for flush 
	private long timeGap;
	//flush time gap,flush counter result
	private long flushTimeGap;
	//keep two window's data
	private KeyBloomFilter kbf1;
	private KeyBloomFilter kbf2;
	private long ignoreCount = 0L;	
	
	public DPIUserStatCounter(String name) {
		super(name);
		
		if (Constants.COUNTER_4G_USER_STAT_1MIN.equals(name)) {
			this.timeGap = 1 * 60 * 1000l;
		} else if (Constants.COUNTER_4G_USER_STAT_5MIN.equals(name)) {
			this.timeGap = 5 * 60 * 1000l;
		} else if (Constants.COUNTER_4G_USER_STAT_10MIN.equals(name)) {
			this.timeGap = 10 * 60 * 1000l;
		} else if (Constants.COUNTER_4G_USER_STAT_1HOUR.equals(name)) {
			this.timeGap = 1 * 3600 * 1000l;
		} else if (Constants.COUNTER_4G_USER_STAT_1DAY.equals(name)) {
			this.timeGap = 24 * 3600 * 1000l;
		}else{
			logger.error("invalid counter name...");
		}
		
		this.kbf1 = new KeyBloomFilter("first bloom filter");
		this.kbf2 = new KeyBloomFilter("second bloom filter");
				
		double t = this.timeGap * ConfigSingleton.getInstance().getCounterFlushTimeGapRatio(name);		
		this.flushTimeGap = (long)t;
		
		//add mdn prefix counter
		super.getCounters().put(MDN_PREFIX_COUNTER,
				new ConcurrentHashMap<String, Count>());
	}
	
	@Override
	public void add(String kafkaKey, Map<String, String> valueData) {
		if (kafkaKey == null && valueData == null) {
			return;
		}

		String mdn = valueData.get(Constants.FIELD_MSISDN);
		long timeStamp = -1L;
		try {
			timeStamp = Long.parseLong(valueData.get(Constants.FIELD_TIMESTAMP));
		} catch (NumberFormatException e) {
			logger.error("invalid timestamp field:" + e.getMessage());
			return;
		}
		
		WindowTime.WinTime winTime = this.getWindowKey(timeStamp);
		if(winTime == null){
			logger.error("invalid counter name=" + super.name);
			return;
		}
		
		long delta1 = timeStamp - this.kbf1.getTimeStamp();
		long delta2 = timeStamp - this.kbf2.getTimeStamp();
		if (this.kbf1.getCount() == -1) {
			//the first time
			this.kbf1.switchKey(winTime);
			this.kbf1.addCounter(mdn, super.getCounters());
			logger.info(super.name + "  " + winTime + " use " + this.kbf1.getName());
		} else if (delta1 >= 0 && delta1 <= this.timeGap) {
			this.kbf1.addCounter(mdn, super.getCounters());
			logger.debug(mdn + " add " + this.kbf1.getName());
		} else if (this.kbf2.getCount() == -1) {
			this.kbf2.switchKey(winTime);
			this.kbf2.addCounter(mdn, super.getCounters());
			logger.info(super.name + "  " + winTime + " use " + this.kbf2.getName());
		} else if (delta2 >= 0 && delta2 <= this.timeGap) {
			this.kbf2.addCounter(mdn, super.getCounters());
			logger.debug(mdn + " add " + this.kbf2.getName());
		} else if (delta1 < 0 && delta2 < 0
				// exclude the first time
				&& this.kbf1.getCount() != -1 && this.kbf2.getCount() != -1) {
			// do nothing,ignore
			logger.debug(mdn + " before kbf1 and kbf2,so ignore " );
			this.ignoreCount++;
			if (this.ignoreCount % 1000 == 0)
				logger.info("ignore " + this.ignoreCount + " message...");
		} else {
			// switch key,find the last key and replace
			KeyBloomFilter kbf = this.getOldestKBF();
			kbf.switchKey(winTime);
			kbf.addCounter(mdn, super.getCounters());
			logger.info(super.name + "  " + winTime + " use " + kbf.getName());
		}
	}
	
	private KeyBloomFilter getOldestKBF() {
		return this.kbf1.getTimeStamp() > this.kbf2.getTimeStamp() ? this.kbf2 : this.kbf1;
	}
	
	private WindowTime.WinTime getWindowKey(long timeStamp){
		if (Constants.COUNTER_4G_USER_STAT_1MIN.equals(super.name)) {
			return WindowTime.get1min(timeStamp);
		} else if (Constants.COUNTER_4G_USER_STAT_5MIN.equals(super.name)) {
			return WindowTime.get5min(timeStamp);
		} else if (Constants.COUNTER_4G_USER_STAT_10MIN.equals(super.name)) {
			return WindowTime.get10min(timeStamp);
		} else if (Constants.COUNTER_4G_USER_STAT_1HOUR.equals(super.name)) {
			return WindowTime.get1hour(timeStamp);
		}else if (Constants.COUNTER_4G_USER_STAT_1DAY.equals(super.name)) {
			return WindowTime.get1day(timeStamp);
		}
		
		return null;
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}
	
	protected class KeyBloomFilter{
		private String key;
		//bloom filter to de-repeat user MDN
		private BloomFilter<String> bloomFilter;
		//switch key,increment it
		private int count = -1;
		//the long timeMillis of key,default is current time
		private long timeStamp;
		private String name;
		
		public KeyBloomFilter(String name){
			this.name = name;
			this.bloomFilter = new BloomFilter<String>(ConfigSingleton.getInstance().getBloomFilterFPP(), 
					ConfigSingleton.getInstance().getBloomFilterExpectedSize());
			this.timeStamp = System.currentTimeMillis();
		}
		
		public void addCounter(String mdn,
				Map<String, Map<String, Count>> counters) {
			if (!this.bloomFilter.contains(mdn)) {
				this.bloomFilter.add(mdn);
				
				String mdnPrefix = parseMdnPrefix(mdn) + Constants.KEY_DELIMITER + key;
				counters.get(MDN_PREFIX_COUNTER)
						.computeIfAbsent(mdnPrefix, k -> new Count()).inc();
				counters.get(DEFALUT_COUNTER)
						.computeIfAbsent(this.key, k -> new Count()).inc();
			}
		}
		
		public synchronized void switchKey(WindowTime.WinTime winTime){
			Preconditions.checkNotNull(winTime, "win time obj is null");
			
			this.key = winTime.getTimeStamp();
			this.timeStamp = winTime.getTimeInMillis();
			this.bloomFilter.clear();
			this.count ++;
		}
		
		public long getTimeStamp() {
			return timeStamp;
		}

		public int getCount() {
			return count;
		}

		public String getName() {
			return name;
		}
	}
}
