package com.gsta.bigdata.stream.flush;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class MDNDayDataRedisFlush extends SimpleRedisFlush {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	public MDNDayDataRedisFlush() {
		super();
	}

	@Override
	public void flush(String counterName,String key, Map<String, String> fieldValues, String timeStamp,long count) {
		if(fieldValues == null)  return;
		
		Jedis jedis = super.jedisPool.getResource();
		//mdn,20161117,count
		//if data flush to redis,cover it
		String mdn = (String)fieldValues.values().toArray()[0];
		jedis.hset(mdn, timeStamp, String.valueOf(count));
		jedis.expire(mdn, super.keyExpire);

		logger.info("counterName={},keyField={},timeStamp={},count={},expireTime=",
				counterName, fieldValues.toString(), timeStamp, count,
				super.keyExpire);

		jedis.close();
	}

}
