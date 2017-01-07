package com.gsta.bigdata.stream;

import java.util.Map;

public interface IFlush {
	/**
	 * 
	 * @param counterName
	 * @param key
	 * @param fieldValues multi field
	 * @param timeStamp
	 * @param count
	 * @param processId
	 * @param ip
	 */
	public void flush(String counterName,String key, Map<String, String> fieldValues,
			String timeStamp, long count, int processId,String ip);

	public void close();
}
