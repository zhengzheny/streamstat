package com.gsta.bigdata.stream;

import java.io.Serializable;

public class Count implements Serializable{
	private static final long serialVersionUID = -750290499072336210L;
	private long cnt = 0;
	@Deprecated
	private boolean finished = false;
	private long timestamp;
	
	public Count(){
		this.timestamp = System.currentTimeMillis();
	}

	public long inc() {
		return ++cnt;
	}

	public long inc(long v) {
		cnt = +v;
		return v;
	}

	public long getCnt() {
		return cnt;
	}
	
	@Deprecated
	public boolean isFinished() {
		return finished;
	}
	@Deprecated
	public void setFinished(boolean finished) {
		this.finished = finished;
	}

	public long getTimestamp() {
		return timestamp;
	}
}
