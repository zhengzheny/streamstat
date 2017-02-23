package com.gsta.bigdata.stream;

import java.util.Map;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.WindowTime;

public class DPI1HourCounter extends AbstractCounter {
	private long flushTimeGap;

	public DPI1HourCounter(String name) {
		super(name);
		
		// 1 hours
		double t = 1 * 3600 * 1000 * ConfigSingleton.getInstance()
				.getCounterFlushTimeGapRatio(super.name);
		this.flushTimeGap = (long) t;
	}

	@Override
	public void add(String kafkaKey, Map<String, String> valueData,String mdn, long timeStamp) {
		if (kafkaKey == null || valueData == null) {
			return;
		}
		
		WindowTime.WinTime winTime = WindowTime.get1hour(timeStamp);
		String key =  winTime.getTimeStamp();
		super.getCounters().computeIfAbsent(key, k -> new Count(winTime.getTimeInMillis())).inc();
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}

}
