package com.gsta.bigdata.stream.counter;


import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.SysUtils;
import com.gsta.bigdata.stream.utils.WindowTime;
import com.gsta.bigdata.stream.Application;

public class AreaDomainCounter extends AbstractCounter {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	// flush time gap,flush counter result
	private long flushTimeGap;
	private String timeGapType;
	// time gap for flush		
	public AreaDomainCounter(String name) {		
		super(name);
		Map<String, Object> conf = ConfigSingleton.getInstance().getCounterConf(name);
		if(conf != null){
			this.timeGapType = (String)conf.getOrDefault("timeGap","1hour");
		}
		// 1 hours
		double t = 1 * 3600 * 1000 * ConfigSingleton.getInstance()
		.getCounterFlushTimeGapRatio(super.name);
		
//		test，5min
//		double t = 300 * 1000 * ConfigSingleton.getInstance()
//				.getCounterFlushTimeGapRatio(super.name);
		this.flushTimeGap = (long) t;
	 }

	@Override
	public void add(String kafkaKey, Map<String, String> valueData, long timeStamp) {
		if (kafkaKey == null || valueData == null) {
			return;
		}	
		String ECGI = valueData.get(Constants.FIELD_ECGI);
//		String mdn = valueData.get(Constants.FIELD_MSISDN);		
		boolean flag = false;
		String area = "";
		WindowTime.WinTime winTime = WindowTime.get1hour(timeStamp);
		String ts = winTime.getTimeStamp();
		for (Entry<String, ArrayList<String>> entry : Application.mapCGI.entrySet()) {
			if(entry.getValue().contains(ECGI)){
				String Area = entry.getKey();
				flag = true;
				area = timeGapType+Area;
			}
			
		}
		if (flag){	
		String domain = valueData.get(Constants.FIELD_Domain);
		String key = area+Constants.KEY_DELIMITER+domain+Constants.KEY_DELIMITER+ts ;
		super.addCount(key);
		super.addCountTimeStamp(key);		
		}
		}
	

	

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}
	
	@Deprecated
	private String getAge(String age) {
		// TODO Auto-generated method stub
		int Age=Integer.valueOf(age);
		
		if (Age >= 20 && Age < 30){
			return "20-30";
		}
		else if (Age >= 30 && Age < 40){
			return "30-40";
		}
		else if (Age >= 40 && Age < 50){
			return "40-50";
		}
		else if (Age >= 50 && Age < 60){
			return "50-60";
		}
		else if (Age < 20){
			return "<20";
		}
		else return ">60";
		
	}
	
}
