package com.gsta.bigdata.stream.groupby;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.CounterCount;

/**
 * 从kafak读取后的消息,构造一个计数器,消息内容放在map中,消息计数放在count中
 * @author tianxq
 *
 */
public class GroupbyCount {
	//同一个key的累积值,如果等于上一次kafka stream处理的进程值,说明全部到位,可以写入kafka
	//计时器从1开始,因为第一次插入默认为1
	private int cnt = 1;
	private CounterCount counterCount;
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public GroupbyCount(CounterCount counterCount) {
		this.counterCount = counterCount;
	}

	public synchronized void groupby(CounterCount counterCount){
		if(counterCount != null){
			try{
				this.counterCount.plusCount(counterCount.getCount());
				this.cnt++;
			}catch(ClassCastException e){
				logger.error("message {} occur error:{}",counterCount.toString(),e.getMessage());
			}
		}
	}
	
	public CounterCount getJsonCount() {
		return counterCount;
	}

	public int getCnt(){
		return this.cnt;
	}
}
