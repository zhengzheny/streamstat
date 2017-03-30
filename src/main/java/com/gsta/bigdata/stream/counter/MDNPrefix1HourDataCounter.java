package com.gsta.bigdata.stream.counter;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.WindowTime;

public class MDNPrefix1HourDataCounter extends AbstractCounter {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private long flushTimeGap;
	private boolean shortmdnprefix = false;

	public MDNPrefix1HourDataCounter(String name) {
		super(name);

		// 1 hours
		double t = 1 * 3600 * 1000 * ConfigSingleton.getInstance()
				.getCounterFlushTimeGapRatio(super.name);
		this.flushTimeGap = (long) t;
		
		Map<String, Object> conf = ConfigSingleton.getInstance().getCounterConf(name);
		if(conf != null){
			this.shortmdnprefix = (boolean)conf.getOrDefault("shortmdnprefix",false);
		}
	}

	@Override
	public void add(String kafkaKey, Map<String, String> valueData,long timeStamp) {
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

		WindowTime.WinTime winTime = WindowTime.get1hour(timeStamp);
		String ts = winTime.getTimeStamp();
		String mdn = valueData.get(Constants.FIELD_MSISDN);
		String key = parseMdnPrefix(mdn,shortmdnprefix) + Constants.KEY_DELIMITER + ts;
		long mdnData = inputOctets + outputOctets;

		super.addCount(key, mdnData);
		super.addCountTimeStamp(key);
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}
}
