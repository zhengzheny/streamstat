package com.gsta.bigdata.stream.counter;

import java.util.Map;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.WindowTime;

public class Field5MinCounter extends AbstractCounter {
	private long flushTimeGap;
	
	public Field5MinCounter(String name) {
		super(name);
		
		//5 minute
		double t = 5 * 60 * 1000 * ConfigSingleton.getInstance()
				.getCounterFlushTimeGapRatio(name);
		this.flushTimeGap = (long) t;
	}

	@Override
	public void add(String kafkaKey, Map<String, String> valueData,String mdn, long timeStamp) {
		if (kafkaKey == null || valueData == null || super.getKeyFields() == null) {
			return;
		}
		
		String key = valueData.get(super.getKeyFields()[0]);
		WindowTime.WinTime winTime = WindowTime.get5min(timeStamp);
		key += Constants.KEY_DELIMITER + winTime.getTimeStamp();
		super.addCount(key);
		super.addCountTimeStamp(key);
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}

}
