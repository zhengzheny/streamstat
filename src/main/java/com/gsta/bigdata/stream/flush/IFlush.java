package com.gsta.bigdata.stream.flush;

import java.util.Map;

public interface IFlush {
	/**
	 * 
	 * @param counterName  计时器名称
	 * @param key 计数的key,由timestamp和keyFields的值组成
	 * @param fieldValues  fields值
	 * @param timeStamp  时间戳
	 * @param count  计数值
	 */
	public void flush(String counterName,String key, Map<String, String> fieldValues,String timeStamp, long count);

	public void close();
}
