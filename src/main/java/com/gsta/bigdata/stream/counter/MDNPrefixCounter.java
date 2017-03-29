package com.gsta.bigdata.stream.counter;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.BloomFilterFactory;
import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.WindowTime;

public class MDNPrefixCounter extends AbstractCounter {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	// flush time gap,flush counter result
	private long flushTimeGap;
	// time gap for flush
	private long timeGap;
	private boolean shortmdnprefix = false;
	private String timeGapType;

	public MDNPrefixCounter(String name) {
		super(name);

		Map<String, Object> conf = ConfigSingleton.getInstance().getCounterConf(name);
		if(conf != null){
			this.timeGapType = (String)conf.getOrDefault("timeGap","5min");
			this.shortmdnprefix = (boolean)conf.getOrDefault("shortmdnprefix",false);
		}
		
		if (Constants.TIME_GAP_5_MIN.equals(timeGapType)) {
			this.timeGap = 5 * 60 * 1000l;
		} else if (Constants.TIME_GAP_1_HOUR.equals(timeGapType)) {
			this.timeGap = 1 * 3600 * 1000l;
		} else if (Constants.TIME_GAP_1_DAY.equals(timeGapType)) {
			this.timeGap = 24 * 3600 * 1000l;
		} else {
			logger.error("invalid counter name...");
		}

		double t = this.timeGap
				* ConfigSingleton.getInstance().getCounterFlushTimeGapRatio(name);
		this.flushTimeGap = (long) t;
		logger.info(name + " timeGap=" + this.timeGapType + ",flushTimeGap=" + this.flushTimeGap);
	}

	@Override
	public void add(String kafkaKey, Map<String, String> valueData, String mdn,long timeStamp) {
		if (kafkaKey == null || valueData == null) {
			return;
		}

		boolean isExist = BloomFilterFactory.getInstance().isExist(
				super.getBloomFilterName(), timeStamp, valueData);
		if (!isExist) {
			WindowTime.WinTime winTime = this.getWindowKey(timeStamp);
			if (winTime != null) {
				String key = super.parseMdnPrefix(mdn,shortmdnprefix)
						+ Constants.KEY_DELIMITER + winTime.getTimeStamp();
				
				super.addCount(key);
				//一天的时间按照当前系统时间的凌晨去算，过期时间还是按照24小时，这样已过零点，会把前一天数据清空
				if(Constants.TIME_GAP_1_DAY.equals(timeGapType)){
					winTime = WindowTime.get1day(System.currentTimeMillis());
					super.addCountTimeStamp(key, winTime.getTimeInMillis());
				}else{
					super.addCountTimeStamp(key);
				}
			}
		}
	}

	private WindowTime.WinTime getWindowKey(long timeStamp) {
		if (Constants.TIME_GAP_5_MIN.equals(timeGapType)) {
			return WindowTime.get5min(timeStamp);
		} else if (Constants.TIME_GAP_1_HOUR.equals(timeGapType)) {
			return WindowTime.get1hour(timeStamp);
		} else if (Constants.TIME_GAP_1_DAY.equals(timeGapType)) {
			return WindowTime.get1day(timeStamp);
		}

		return null;
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}
}
