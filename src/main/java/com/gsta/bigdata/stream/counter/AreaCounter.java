package com.gsta.bigdata.stream.counter;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.Application;
import com.gsta.bigdata.stream.BloomFilterFactory;
import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.WindowTime;
/*------------------
 * 实现对某个区域的监控。
 * 要增加监控，需要先在配置文件增加布隆过滤器（不需要可以跳过），然后去程序启动脚本增加要启动的过滤器（不需要可以跳过），
 * 再到这个文件cgi.txt增加ecgi list,匹配布隆过滤器以及区域名称，即可实现监控。
 */
public class AreaCounter extends AbstractCounter {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	// flush time gap,flush counter result
	private long flushTimeGap;
	// time gap for flush
	private long repeatCount = 1;
	private long timeGap;
	private String timeGapType;
	
	public AreaCounter(String name) {
		super(name);
		Map<String, Object> conf = ConfigSingleton.getInstance().getCounterConf(name);
		if(conf != null){
			this.timeGapType = (String)conf.getOrDefault("timeGap","5min");
		}
		
		if (Constants.TIME_GAP_5_MIN.equals(timeGapType)) {
			this.timeGap = 5 * 60 * 1000l;
		} else if (Constants.TIME_GAP_1_HOUR.equals(timeGapType)) {
			this.timeGap = 1 * 3600 * 1000l;
		} else{
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
		String ECGI = valueData.get(Constants.FIELD_ECGI);
		boolean isExist = true;
		boolean flag = false;
		String selectedFilter= "";
		String area = "";
		for (Entry<String, ArrayList<String>> entry : Application.mapCGI.entrySet()) {
			if(entry.getValue().contains(ECGI)){
				String Area = entry.getKey();
				flag = true;
				selectedFilter = timeGapType+Area+"-mdn-bloomFilter";
				area = timeGapType+Area;
			}  
		}
		if (flag){	
//			logger.info("start:"+ECGI+" selectedFilter:"+selectedFilter);
			WindowTime.WinTime winTime = this.getWindowKey(timeStamp);
			String ts = winTime.getTimeStamp();
			isExist = BloomFilterFactory.getInstance().isExist(
					selectedFilter, timeStamp, valueData);
				if (!isExist) {
					String key = area+Constants.KEY_DELIMITER+"userstat" + Constants.KEY_DELIMITER+ts;
					super.addCount(key);
					super.addCountTimeStamp(key);
				}else
					{
				this.repeatCount++;
					}
				if(this.repeatCount % 1000 == 0){
				logger.info("{} has repeat count={}",selectedFilter+" userstat",this.repeatCount);
				}
								
//				userdata:流量统计
				long inputOctets = 0, outputOctets = 0;
				try {
					inputOctets = Long.parseLong(valueData.get(Constants.FIELD_InputOctets));
					outputOctets = Long.parseLong(valueData.get(Constants.FIELD_OutputOctets));
					} catch (NumberFormatException e) {
					logger.error(e.getMessage());
					return;
					}
				long mdnData = inputOctets + outputOctets;
				String key =  area+Constants.KEY_DELIMITER+"userdata" + Constants.KEY_DELIMITER+ts;
				super.addCount(key, mdnData);
				super.addCountTimeStamp(key);
				BloomFilterFactory.getInstance().add(area,timeStamp,valueData);
		}
		
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}
	private WindowTime.WinTime getWindowKey(long timeStamp){
		if (Constants.TIME_GAP_5_MIN.equals(timeGapType)) {
			return WindowTime.get5min(timeStamp);
		} else if (Constants.TIME_GAP_1_HOUR.equals(timeGapType)) {
			return WindowTime.get1hour(timeStamp);
		}
		
		return null;
	}
}
