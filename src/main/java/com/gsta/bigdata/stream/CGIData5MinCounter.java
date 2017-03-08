package com.gsta.bigdata.stream;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.WindowTime;

public class CGIData5MinCounter extends AbstractCounter {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private long flushTimeGap;
	
	public CGIData5MinCounter(String name) {
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

		String CGI = valueData.get(super.getKeyFields()[0]);
		long inputOctets = 0, outputOctets = 0;
		try {
			inputOctets = Long.parseLong(valueData.get(Constants.FIELD_InputOctets));
			outputOctets = Long.parseLong(valueData.get(Constants.FIELD_OutputOctets));
		} catch (NumberFormatException e) {
			logger.error(e.getMessage());
			return;
		}

		WindowTime.WinTime winTime = WindowTime.get5min(timeStamp);
		String ts = winTime.getTimeStamp();
		String key = CGI + Constants.KEY_DELIMITER + ts;
		long data = inputOctets + outputOctets;

		super.getCounters().computeIfAbsent(key, k -> new Count(winTime.getTimeInMillis())).inc(data);
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}
}
