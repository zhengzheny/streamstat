package com.gsta.bigdata.stream.counter;

import java.io.Serializable;

/**
 * 计数器的创建时间
 * @author tianxq
 *
 */
public class CountTimeStamp implements Serializable {
	private static final long serialVersionUID = 1608955684912419412L;
	private long timestamp;

	public CountTimeStamp() {
		this.timestamp = System.currentTimeMillis();
	}

	public CountTimeStamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getTimestamp() {
		return timestamp;
	}

}
