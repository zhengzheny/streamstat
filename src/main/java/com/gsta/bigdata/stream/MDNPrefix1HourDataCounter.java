package com.gsta.bigdata.stream;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.WindowTime;

public class MDNPrefix1HourDataCounter extends AbstractCounter {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private long flushTimeGap;

	public MDNPrefix1HourDataCounter(String name) {
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

		String mdn = valueData.get(Constants.FIELD_MSISDN);
		long timeStamp = -1L, inputOctets = 0, outputOctets = 0;
		try {
			timeStamp = Long.parseLong(valueData.get(Constants.FIELD_TIMESTAMP));
			inputOctets = Long.parseLong(valueData.get(Constants.FIELD_InputOctets));
			outputOctets = Long.parseLong(valueData.get(Constants.FIELD_OutputOctets));
		} catch (NumberFormatException e) {
			logger.error(e.getMessage());
			return;
		}

		String ts = WindowTime.get1hour(timeStamp).getTimeStamp();
		String key = parseMdnPrefix(mdn) + Constants.KEY_DELIMITER + ts;
		long mdnData = inputOctets + outputOctets;

		super.getDefaultCounter().computeIfAbsent(key, k -> new Count()).inc(mdnData);
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}
}
