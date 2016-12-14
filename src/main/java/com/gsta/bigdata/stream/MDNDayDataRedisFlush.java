package com.gsta.bigdata.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class MDNDayDataRedisFlush extends SimpleRedisFlush {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	public MDNDayDataRedisFlush() {
		super();
	}

	@Override
	public void flush(String counterName, String keyField, String timeStamp,
			long count, int processId) {
		if(keyField == null)  return;
		
		Jedis jedis = super.jedisPool.getResource();
		//mdn,20161117,count
		//if data flush to redis,cover it
		jedis.hset(keyField, timeStamp, String.valueOf(count));
		jedis.expire(keyField, super.keyExpire);
		
		logger.info("counterName=" + counterName + ",keyField=" + keyField
				+ ",processId=" + processId + ",timeStamp=" + timeStamp
				+ ",count=" + count + ",expireTime=" + super.keyExpire);
	
		jedis.close();
	}

}
