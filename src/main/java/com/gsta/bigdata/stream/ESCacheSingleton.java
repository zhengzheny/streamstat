package com.gsta.bigdata.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ESCacheSingleton {
	private final static ESCacheSingleton singleton = new ESCacheSingleton();
	private Map<String, HashMap<String,Object>> requests = new ConcurrentHashMap<String, HashMap<String,Object>>();

	private ESCacheSingleton() {
		
	}
	
	public void addRequest(String key,HashMap<String,Object> map){
		if(key != null && map != null){
			this.requests.put(key, map);
		}
	}

	public Map<String, HashMap<String, Object>> getRequests() {
		return requests;
	}

	public static ESCacheSingleton getSingleton() {
		return singleton;
	}
}
