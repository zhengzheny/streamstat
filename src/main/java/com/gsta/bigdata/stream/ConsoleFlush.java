package com.gsta.bigdata.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsoleFlush implements IFlush {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Override
	public void close() {
		
	}

	@Override
	public void flush(String counterName, String keyField, String timeStamp,
			long count, int processId) {
		logger.info("counterName=" + counterName + ",keyField=" + keyField
				+ ",processId=" + processId + ",timeStamp=" + timeStamp + ",count=" + count);
	}

}
