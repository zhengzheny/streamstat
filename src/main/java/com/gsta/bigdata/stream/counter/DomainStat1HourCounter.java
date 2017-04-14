package com.gsta.bigdata.stream.counter;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.SysUtils;
import com.gsta.bigdata.stream.utils.WindowTime;

public class DomainStat1HourCounter extends AbstractCounter {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	// flush time gap,flush counter result
	private long flushTimeGap;

	public DomainStat1HourCounter(String name) {
		super(name);
		double t = 1 * 3600 * 1000 * ConfigSingleton.getInstance()
				.getCounterFlushTimeGapRatio(super.name);
		this.flushTimeGap = (long) t;
	}

	@Override
	public void add(String kafkaKey, Map<String, String> valueData,long timeStamp) {
		if (kafkaKey == null || valueData == null) {
			return;
		}
		String Domain ="";
		try{
		 Domain = SysUtils.getLevel3Domain(valueData.get("Domain"));
		}catch (NumberFormatException e) {
			logger.error(e.getMessage()+valueData.get("Domain"));
			return;
		}
		WindowTime.WinTime winTime = WindowTime.get1hour(timeStamp);
		String key = Domain + Constants.KEY_DELIMITER + winTime.getTimeStamp();;
		super.addCount(key);
		super.addCountTimeStamp(key);

	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}

	public static void main(String[] args){
		Map<String, String> valueData = new HashMap<String, String>();
		valueData.put("Domain", "szextshort.weixin.qq.com");
		String domain=valueData.get("Domain");
		//domain="api.weibo.cn";
		//domain = "qq.com";
		//domain="182.254.116.117:80";
		//domain="182.254.116.117";
		//domain = "amdc.m.taobao.com:8090";
		
		System.out.println(SysUtils.getLevel3Domain(domain));
	}
}
