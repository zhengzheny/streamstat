package com.gsta.bigdata.stream;

import redis.clients.jedis.Jedis;

public class MDNDayDataRedisFlush extends SimpleRedisFlush {
	public MDNDayDataRedisFlush() {
		super();
	}

	@Override
	public void flush(String counterName, String keyField, String timeStamp,
			long count, int processId) {
		Jedis jedis = super.jedisPool.getResource();
		//mdn,20161117,count
		//if data flush to redis,cover it
		jedis.hset(keyField, timeStamp, String.valueOf(count));
		jedis.expire(keyField, super.keyExpire);
		jedis.close();
	}

}
