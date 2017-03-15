package com.gsta.bigdata.stream.counter;

import java.io.Serializable;

public class Count implements Serializable{
	private static final long serialVersionUID = -750290499072336210L;
	private long cnt = 0;
	
	public Count(){
	}

	public long inc() {
		return ++cnt;
	}

	public long inc(long v) {
		cnt += v;
		return v;
	}

	public long getCnt() {
		return cnt;
	}
}
