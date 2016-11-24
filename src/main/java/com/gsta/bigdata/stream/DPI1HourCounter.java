package com.gsta.bigdata.stream;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.WindowTime;

public class DPI1HourCounter extends AbstractCounter {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private long flushTimeGap;

	public DPI1HourCounter(String name) {
		super(name);
		
		// 1 hours
		double t = 1 * 3600 * 1000 * ConfigSingleton.getInstance()
				.getCounterFlushTimeGapRatio(super.name);
		this.flushTimeGap = (long) t;
	}

	@Override
	public void add(String kafkaKey, Map<String, String> valueData) {
		if (kafkaKey == null && valueData == null) {
			return;
		}
		
		long timeStamp = -1L;
		try {
			timeStamp = Long.parseLong(valueData.get(Constants.FIELD_TIMESTAMP));
		} catch (NumberFormatException e) {
			logger.error(e.getMessage());
			return;
		}
		
		String key =  WindowTime.get1hour(timeStamp).getTimeStamp();
		super.getDefaultCounter().computeIfAbsent(key, k -> new Count()).inc();
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}

}
