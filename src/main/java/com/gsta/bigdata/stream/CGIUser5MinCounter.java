package com.gsta.bigdata.stream;

import java.util.Map;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.WindowTime;

public class CGIUser5MinCounter extends AbstractCounter {
	private long flushTimeGap;

	public CGIUser5MinCounter(String name) {
		super(name);
		// 5 minute
		double t = 5 * 60 * 1000 * ConfigSingleton.getInstance()
				.getCounterFlushTimeGapRatio(name);
		this.flushTimeGap = (long) t;
	}

	@Override
	public void add(String kafkaKey, Map<String, String> valueData, String mdn,
			long timeStamp) {
		if (kafkaKey == null || valueData == null || super.getKeyFields() == null) {
			return;
		}

		boolean isExist = BloomFilterFactory.getInstance().isExist(
				super.getBloomFilterName(), timeStamp, mdn);
		if (!isExist) {
			String key = valueData.get(super.getKeyFields()[0]);
			WindowTime.WinTime winTime = WindowTime.get5min(timeStamp);
			key = key + Constants.KEY_DELIMITER + winTime.getTimeStamp();
			super.getCounters().computeIfAbsent(key, k -> new Count(winTime.getTimeInMillis())).inc();
		}
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}
}
