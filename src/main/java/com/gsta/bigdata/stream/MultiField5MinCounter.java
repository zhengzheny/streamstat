package com.gsta.bigdata.stream;

import java.util.Map;
import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.WindowTime;

public class MultiField5MinCounter extends AbstractCounter {
	private long flushTimeGap;
	
	public MultiField5MinCounter(String name) {
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
		
		String key = "";
		for (String field : super.getKeyFields()) {
			key += valueData.get(field);
			key += Constants.KEY_DELIMITER;
		}
		key += WindowTime.get5min(timeStamp).getTimeStamp();
		super.getCounters().computeIfAbsent(key, k -> new Count()).inc();
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}

}
