package com.gsta.bigdata.stream.flush;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.CounterCacheSingleton;
import com.gsta.bigdata.stream.CounterCount;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.SysUtils;

public class ElasticsearchFlush implements IFlush {
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	private final static String DAY_DELI = "-";
	private final static String HOUR_DELI = ":";
	//only for test
	@SuppressWarnings("unused")
	private int processId;
	@SuppressWarnings("unused")
	private String ip;
	
	public ElasticsearchFlush() {
		this.processId = SysUtils.getProcessID();
		this.ip = SysUtils.getLastIp();
	}

	@Override
	/**
	 * 把结果数据以json格式写入kafka
	 */
	public void flush(String counterName, String key,
			Map<String, String> fieldValues, String timeStamp, long count) {
		Map<String,Object> map = new HashMap<String,Object>();
		
		if(fieldValues != null && fieldValues.size() > 0)  {
			map.putAll(fieldValues);
		}
		map.put(Constants.OUTPUT_FIELD_TIMESTAMP,this.formatTimestamp(timeStamp));
		//map.put(Constants.OUTPUT_FIELD_PROCESSID, processId);
		//map.put(Constants.OUTPUT_FIELD_IP, ip);
		map.put(Constants.OUTPUT_FIELD_COUNTER_NAME, counterName);
		map.put(Constants.OUTPUT_FIELD_KEY, key);
		
		CounterCount jsonCount = new CounterCount(count,map);
		CounterCacheSingleton.getSingleton().offer(jsonCount);
	}

	@Override
	public void close() {
		
	}

	/**
	 * 按照Elasticsearch格式写入
	 * @param key
	 * @return
	 */
	private String formatTimestamp(String key) {
		if (key != null) {
			String ret = null;
			switch (key.length()) {
			case 8:
				// yyyyMMdd
				ret = key.substring(0, 4) + DAY_DELI + key.substring(4, 6)
						+ DAY_DELI + key.substring(6, 8) + "T00:00:00+0800";
				break;
			case 10:
				// yyyyMMddHH
				ret = key.substring(0, 4) + DAY_DELI + key.substring(4, 6)
						+ DAY_DELI + key.substring(6, 8) + "T"
						+ key.substring(8, 10) + ":00:00+0800";
				break;
			case 12:
				// yyyyMMddHHmm
				ret = key.substring(0, 4) + DAY_DELI + key.substring(4, 6)
						+ DAY_DELI + key.substring(6, 8) + "T"
						+ key.substring(8, 10) + HOUR_DELI
						+ key.substring(10, 12) + HOUR_DELI + "00+0800";
				break;
			}

			return ret;
		}// end if

		return null;
	}
}
