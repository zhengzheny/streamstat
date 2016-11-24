package com.gsta.bigdata.stream;

import static com.gsta.bigdata.stream.utils.ConfigSingleton.getInstance;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class SimpleRedisFlush implements IFlush {
	protected JedisPool jedisPool;
	protected int keyExpire;
	
	public SimpleRedisFlush(){
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal((int)getInstance().getRedisProperties().get("maxTotal"));
		poolConfig.setMaxIdle((int)getInstance().getRedisProperties().get("maxIdle"));
		poolConfig.setMinIdle((int)getInstance().getRedisProperties().get("minIdle"));
		poolConfig.setNumTestsPerEvictionRun((int)getInstance().getRedisProperties().get("numTestsPerEvictionRun"));
		poolConfig.setTimeBetweenEvictionRunsMillis((int)getInstance().getRedisProperties().get("timeBetweenEvictionRunsMillis"));
		poolConfig.setMinEvictableIdleTimeMillis((int)getInstance().getRedisProperties().get("minEvictableIdleTimeMillis"));
		poolConfig.setSoftMinEvictableIdleTimeMillis((int)getInstance().getRedisProperties().get("softMinEvictableIdleTimeMillis"));
		poolConfig.setMaxWaitMillis((int)getInstance().getRedisProperties().get("maxWaitMillis"));
		poolConfig.setTestOnBorrow((boolean)getInstance().getRedisProperties().get("testOnBorrow"));
		poolConfig.setTestWhileIdle((boolean)getInstance().getRedisProperties().get("testWhileIdle"));
		poolConfig.setTestOnReturn((boolean)getInstance().getRedisProperties().get("testOnReturn"));
		poolConfig.setBlockWhenExhausted((boolean)getInstance().getRedisProperties().get("blockWhenExhausted"));
	
		String host = (String)getInstance().getRedisProperties().get("host");
		int port = (int)getInstance().getRedisProperties().get("port");
		this.jedisPool = new JedisPool(poolConfig,host,port);
		
		this.keyExpire = (int)getInstance().getRedisProperties().get("keyExpire");
	}
	
	@Override
	public void flush(String counterName, String keyField, String timeStamp,
			long count, int processId) {
		Jedis jedis = jedisPool.getResource();
		jedis.set(timeStamp, String.valueOf(count));
		jedis.expire(timeStamp, this.keyExpire);
		jedis.close();
	}

	@Override
	public void close() {
		if(this.jedisPool != null){
			this.jedisPool.close();
		}
	}
}
