package com.gsta.bigdata.stream;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.WindowTime;

public class Field5MinCounter extends AbstractCounter {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private long flushTimeGap;
	private String statField;
	
	public Field5MinCounter(String name) {
		super(name);
		
		//5 minute
		double t = 5 * 60 * 1000 * ConfigSingleton.getInstance()
				.getCounterFlushTimeGapRatio(name);
		this.flushTimeGap = (long) t;
		this.statField = ConfigSingleton.getInstance().getCounterStatField(name);
	}

	@Override
	public void add(String kafkaKey, Map<String, String> valueData) {
		if (kafkaKey == null && valueData == null) {
			return;
		}
		
		String key = valueData.get(this.statField);
		long timeStamp = -1L;
		try {
			timeStamp = Long.parseLong(valueData.get(Constants.FIELD_TIMESTAMP));
		} catch (NumberFormatException e) {
			logger.error(e.getMessage());
			return;
		}
		
		key = key + Constants.KEY_DELIMITER
				+ WindowTime.get5min(timeStamp).getTimeStamp();
		super.getDefaultCounter().computeIfAbsent(key, k -> new Count()).inc();
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}

}
