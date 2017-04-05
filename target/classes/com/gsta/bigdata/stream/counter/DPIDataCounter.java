package com.gsta.bigdata.stream.counter;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.WindowTime;

public class DPIDataCounter extends AbstractCounter {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private long flushTimeGap;
	private long timeGap;
	private String timeGapType;

	public DPIDataCounter(String name) {
		super(name);		
		Map<String, Object> conf = ConfigSingleton.getInstance().getCounterConf(name);
		if(conf != null){
			this.timeGapType = (String)conf.getOrDefault("timeGap","1hour");
		}
		
		if (Constants.TIME_GAP_5_MIN.equals(timeGapType)) {
			this.timeGap = 5 * 60 * 1000l;
		} else if (Constants.TIME_GAP_1_HOUR.equals(timeGapType)) {
			this.timeGap = 1 * 3600 * 1000l;
		} else if (Constants.TIME_GAP_1_DAY.equals(timeGapType)) {
			this.timeGap = 24 * 3600 * 1000l;
		}else{
			logger.error("invalid counter name...");
		}
		
		double t = this.timeGap * ConfigSingleton.getInstance().getCounterFlushTimeGapRatio(name);		
		this.flushTimeGap = (long)t;
		logger.info(name + " timeGap=" + this.timeGapType + ",flushTimeGap=" + this.flushTimeGap);
	}

	@Override
	public void add(String kafkaKey, Map<String, String> valueData, long timeStamp) {
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
		WindowTime.WinTime winTime = this.getWindowKey(timeStamp);
		if (winTime != null) {
			String key = winTime.getTimeStamp();
			long mdnData = inputOctets + outputOctets;
			super.addCount(key, mdnData);
			super.addCountTimeStamp(key);
		}
	}
	
	private WindowTime.WinTime getWindowKey(long timeStamp){
		if (Constants.TIME_GAP_5_MIN.equals(timeGapType)) {
			return WindowTime.get5min(timeStamp);
		} else if (Constants.TIME_GAP_1_HOUR.equals(timeGapType)) {
			return WindowTime.get1hour(timeStamp);
		}else if (Constants.TIME_GAP_1_DAY.equals(timeGapType)) {
			return WindowTime.get1day(timeStamp);
		}
		
		return null;
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}
}
