package com.gsta.bigdata.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CounterCacheSingleton {
	private final static CounterCacheSingleton singleton = new CounterCacheSingleton();
	private Map<String, HashMap<String,Object>> requests = new ConcurrentHashMap<String, HashMap<String,Object>>();

	private CounterCacheSingleton() {
		
	}
	
	public void addRequest(String key,HashMap<String,Object> map){
		if(key != null && map != null){
			this.requests.put(key, map);
		}
	}

	public Map<String, HashMap<String, Object>> getRequests() {
		return requests;
	}

	public static CounterCacheSingleton getSingleton() {
		return singleton;
	}
}
