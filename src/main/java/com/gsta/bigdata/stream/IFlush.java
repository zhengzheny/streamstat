package com.gsta.bigdata.stream;

public interface IFlush {
	public void flush(String counterName,String keyField,String timeStamp,long count,int processId);
	public void close();
}
