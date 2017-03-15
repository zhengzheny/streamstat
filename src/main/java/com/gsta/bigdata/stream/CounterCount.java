package com.gsta.bigdata.stream;

import java.io.Serializable;
import java.util.Map;

import com.gsta.bigdata.stream.utils.Constants;

/**
 * 存放计数后的结果,count便于groupby累积所用,其他字段统一放入map中
 * @author tianxq
 *
 */
public class CounterCount implements Serializable {
	private static final long serialVersionUID = -1294630685484843669L;
	public long count;
	public Map<String, Object> map;

	public CounterCount(long count, Map<String, Object> map) {
		super();
		this.count = count;
		this.map = map;
	}
	
	public String getCounterName(){
		return (String)map.get(Constants.OUTPUT_FIELD_COUNTER_NAME);
	}
	
	/**
	 * groupby的统计量累加
	 * @param cnt
	 */
	public void plusCount(long cnt){
		this.count += cnt;
	}
	
	public long getCount() {
		return count;
	}

	public String getValue(String key){
		return (String)map.get(key);
	}
	
	public Map<String, Object> getMap() {
		return map;
	}

	public String toString(){
		return map.toString() + ",count=" + count;
	}
}
