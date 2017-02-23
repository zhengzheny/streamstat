package com.gsta.bigdata.stream;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.Constants;

public class ElasticsearchFlush implements IFlush {
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	private final static String DAY_DELI = "-";
	private final static String HOUR_DELI = ":";
	
	public ElasticsearchFlush() {
		
	}

	@Override
	public void flush(String counterName, String key,
			Map<String, String> fieldValues, String timeStamp, long count,
			int processId, String ip) {
		HashMap<String,Object> map = new HashMap<String,Object>();
		
		if(fieldValues != null && fieldValues.size() > 0)  {
			map.putAll(fieldValues);
		}
		map.put("timeStamp",this.formatTimestamp(timeStamp));
		map.put("count", count);
		map.put("processId", processId);
		map.put("ip", ip);
		map.put("counterName", counterName);
		String tempKey = key + Constants.KEY_DELIMITER + processId + 
				Constants.KEY_DELIMITER + ip;
		
		long t = System.currentTimeMillis();
		String s = String.valueOf(t);
		tempKey += Constants.KEY_DELIMITER + s.substring(s.length()-5);
		
		String reqKey = counterName + Constants.REQUEST_KEY_DELIMITER + tempKey;
		ESCacheSingleton.getSingleton().addRequest(reqKey, map);
	}

	@Override
	public void close() {
		
	}

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
