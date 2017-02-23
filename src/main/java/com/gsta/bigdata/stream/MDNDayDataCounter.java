package com.gsta.bigdata.stream;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.WindowTime;

public class MDNDayDataCounter extends AbstractCounter {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private long flushTimeGap;	

	public MDNDayDataCounter(String name) {
		super(name);

		// 24 hours
		double t = 24 * 3600 * 1000 * ConfigSingleton.getInstance()
				.getCounterFlushTimeGapRatio(super.name);
		this.flushTimeGap = (long) t;
	}

	@Override
	public void add(String kafkaKey,Map<String, String> valueData,String mdn, long timeStamp) {
		if (kafkaKey == null || valueData == null) {
			return;
		}
		
		long inputOctets = 0, outputOctets = 0;
		try {
			inputOctets = Long.parseLong(valueData.get(Constants.FIELD_InputOctets));
			outputOctets = Long.parseLong(valueData.get(Constants.FIELD_OutputOctets));
		} catch (NumberFormatException e) {
			logger.error(e.getMessage());
			return;
		}
		
		WindowTime.WinTime winTime = WindowTime.get1day(timeStamp);
		String ts = winTime.getTimeStamp();
		String key = mdn + Constants.KEY_DELIMITER + ts;
	
		long mdnData = inputOctets + outputOctets;
		super.getCounters().computeIfAbsent(key, k -> new Count(winTime.getTimeInMillis())).inc(mdnData);
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}
}
