package com.gsta.bigdata.stream;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 计数器队列,各种flush到队列中来,kafkawriter从队列读取写入kafka
 * @author tianxq
 *
 */
public class CounterCacheSingleton {
	private final static CounterCacheSingleton singleton = new CounterCacheSingleton();
	BlockingQueue<Object> queue = new LinkedBlockingQueue<>();

	private CounterCacheSingleton() {

	}

	public void offer(Object object) {
		this.queue.offer(object);
	}

	public Object poll() {
		return this.queue.poll();
	}
	
	public int getPoolSize(){
		return this.queue.size();
	}

	// 遗留方法，没有用途
	@Deprecated
	public Map<String, Map<String, Object>> getRequests() {
		return null;
	}

	public static CounterCacheSingleton getSingleton() {
		return singleton;
	}
}
