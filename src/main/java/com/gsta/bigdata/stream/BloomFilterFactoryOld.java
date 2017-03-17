package com.gsta.bigdata.stream;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.WindowTime;

@Deprecated
/**
 * 全局共享布隆过滤
 * @author tianxq
 *
 */
public class BloomFilterFactoryOld {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private static BloomFilterFactoryOld singleton = new BloomFilterFactoryOld();
	private Map<String, WindowBloomFilter> bloomFilters = new HashMap<String, WindowBloomFilter>();
	private int expectedSize = 1000;
	private double falsePositiveProbability = 0.1;
	//是否开启布隆过滤开关
	private boolean filter = false;
	private List<String> names;

	@SuppressWarnings("unchecked")
	private BloomFilterFactoryOld() {
		Map<String, Object> conf = ConfigSingleton.getInstance().getBloomFilter();
		if (conf != null) {
			this.falsePositiveProbability = (double) conf.getOrDefault(
					"falsePositiveProbability", 0.1);
			this.expectedSize = (int) conf.getOrDefault("expectedSize", 1000);
			this.filter = (boolean)conf.get("filter");
			this.names = (List<String>) conf.get("name");
		} else {
			logger.error("invalid bloom filter config...");
		}
	}

	public void init() {
		if (this.filter && this.names != null) {
			for (String name : names) {
				WindowBloomFilter windowBloomFilter = new WindowBloomFilter(
						this.getFilterTimeGap(name), this.expectedSize,
						this.falsePositiveProbability);
				this.bloomFilters.put(name, windowBloomFilter);
			}
		}
	}

	/**
	 * 向布隆过滤器增加数据
	 * @param timeStamp - 时间戳,根据时间戳找到对应的布隆过滤其
	 * @param mdn - 电话号码
	 */
	public void add(long timeStamp,String mdn){
		if(this.filter){
			for (Map.Entry<String, WindowBloomFilter> mapEntry : this.bloomFilters.entrySet()) {
				String filterName = mapEntry.getKey();
				WindowBloomFilter wbf = mapEntry.getValue();
				
				WindowTime.WinTime winTime = this.getWindowKey(filterName,timeStamp);
				wbf.add(timeStamp, winTime, mdn);
			}
		}
	}
	
	/**
	 * 判断号码是否在过滤器中
	 * @param filterName
	 * @param timeStamp
	 * @param mdn
	 * @return
	 */
	public boolean isExist(String filterName,long timeStamp,String mdn){
		WindowBloomFilter windowBloomFilter = this.bloomFilters.get(filterName);
		if (windowBloomFilter != null) {
			return windowBloomFilter.isExist(timeStamp, mdn);
		}
		
		return false;
	}

	private long getFilterTimeGap(String filterName) {
		if (Constants.BLOOM_FILTER_5MIN.equals(filterName)) {
			return 5 * 60 * 1000l;
		} else if (Constants.BLOOM_FILTER_1HOUR.equals(filterName)) {
			return 1 * 3600 * 1000l;
		} else if (Constants.BLOOM_FILTER_1DAY.equals(filterName)) {
			return 24 * 3600 * 1000l;
		}

		return 5 * 60 * 1000l;
	}
	
	private WindowTime.WinTime getWindowKey(String filterName,long timeStamp){
		if (Constants.BLOOM_FILTER_5MIN.equals(filterName)) {
			return WindowTime.get5min(timeStamp);
		} else if (Constants.BLOOM_FILTER_1HOUR.equals(filterName)) {
			return WindowTime.get1hour(timeStamp);
		}else if (Constants.BLOOM_FILTER_1DAY.equals(filterName)) {
			return WindowTime.get1day(timeStamp);
		}
		
		return null;
	}

	public static BloomFilterFactoryOld getInstance() {
		return singleton;
	}

}
