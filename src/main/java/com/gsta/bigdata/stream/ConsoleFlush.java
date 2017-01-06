package com.gsta.bigdata.stream;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsoleFlush implements IFlush {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Override
	public void close() {
		
	}

	@Override
	public void flush(String counterName, Map<String, String> fieldValues, String timeStamp,
			long count, int processId) {
		logger.info("counterName=" + counterName + ",keyField=" + fieldValues.toString()
				+ ",processId=" + processId + ",timeStamp=" + timeStamp + ",count=" + count);
	}

}
