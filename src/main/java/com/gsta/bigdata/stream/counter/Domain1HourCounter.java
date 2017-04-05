package com.gsta.bigdata.stream.counter;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.BloomFilterFactory;
import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.WindowTime;

public class Domain1HourCounter extends AbstractCounter {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	// flush time gap,flush counter result
	private long flushTimeGap;
	// time gap for flush
	private long repeatCount = 1;
		
	public Domain1HourCounter(String name) {
		super(name);
		
		// 1 hours
		double t = 1 * 3600 * 1000 * ConfigSingleton.getInstance()
				.getCounterFlushTimeGapRatio(super.name);
//          test,5min
/**		double t = 300 * 1000 * ConfigSingleton.getInstance()
				.getCounterFlushTimeGapRatio(super.name);
*/	
		this.flushTimeGap = (long) t;
	}

	@Override
	public void add(String kafkaKey, Map<String, String> valueData, long timeStamp) {
		if (kafkaKey == null || valueData == null) {
			return;
		}

		boolean isExist = BloomFilterFactory.getInstance().isExist(
				super.getBloomFilterName(), timeStamp, valueData);
		if (!isExist) {			
				WindowTime.WinTime winTime = WindowTime.get1hour(timeStamp);
				String key =  winTime.getTimeStamp();
				super.addCount(key);
				super.addCountTimeStamp(key);
		}else
		{
			this.repeatCount++;
		}
		if(this.repeatCount % 1000 == 0){
			logger.info("{} has repeat count={}",this.name,this.repeatCount);
		}
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}

}
