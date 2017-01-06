package com.gsta.bigdata.stream;

import java.util.Map;

public interface IFlush {
	/**
	 * 
	 * @param counterName
	 * @param fieldValues - multi field
	 * @param timeStamp
	 * @param count
	 * @param processId
	 */
	public void flush(String counterName, Map<String, String> fieldValues,
			String timeStamp, long count, int processId);

	public void close();
}
