package com.gsta.bigdata.stream.counter;

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
	public void add(String kafkaKey,Map<String, String> valueData,long timeStamp) {
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
		String mdn = valueData.get(Constants.FIELD_MSISDN);
		String key = mdn + Constants.KEY_DELIMITER + ts;
	
		long mdnData = inputOctets + outputOctets;
		super.addCount(key, mdnData);
		
		//一天的时间按照当前系统时间的凌晨去算，过期时间还是按照24小时，这样已过零点，会把前一天数据清空
		winTime = WindowTime.get1day(System.currentTimeMillis());
		super.addCountTimeStamp(key, winTime.getTimeInMillis());
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}
}
