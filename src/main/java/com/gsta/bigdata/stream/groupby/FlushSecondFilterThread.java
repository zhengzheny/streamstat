package com.gsta.bigdata.stream.groupby;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.CounterCacheSingleton;
import com.gsta.bigdata.stream.counter.CountTimeStamp;

/**
 * 定期扫描计数器,如有有到期的,就写到kafka,对于那种分区不均的,等待一段时间后写出去
 * 
 * @author tianxq
 *
 */
public class FlushSecondFilterThread implements Runnable {
	private SecondFilterCounter counter;
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	// 单位为秒,默认保留5分钟
	private long flushTime;

	public FlushSecondFilterThread(SecondFilterCounter counter, int flushTime) {
		this.counter = counter;
		this.flushTime = flushTime * 1000L;
	}

	@Override
	public void run() {
		while (true) {
			if (counter == null) {
				logger.error("flush groupby counters is null");
				break;
			}
			int flushCount = 0;
			int counterSize = counter.getCountersTimeStamp().size();
			boolean flag = false;
			// 扫描CountersTimeStamp,发现超时,将会从Counters中获取count进行flush,并删除count
			for (Map.Entry<String, CountTimeStamp> mapEntry : counter.getCountersTimeStamp().entrySet()) {
				String key = mapEntry.getKey();
				if (key == null) continue;

				CountTimeStamp countTimeStamp = mapEntry.getValue();
				// 当前系统时间和计数器的创建时间的差超过设定的阀值，flush出去并从集合中删除
				long deltaTime = System.currentTimeMillis() - countTimeStamp.getTimestamp();
				if (deltaTime >= this.flushTime) {
					GroupbyCount groupbyCount = counter.getCounters().get(key);
					CounterCacheSingleton.getSingleton().offer(groupbyCount);
					counter.getCounters().remove(key);
					counter.getCountersTimeStamp().remove(key);
					flushCount++;
				}
				flag = true;
			}// end for results

			if (flag) {
				logger.info("flush counterSize={},flushCount={}", counterSize,flushCount);
			}
			try {
				Thread.sleep(500L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}// end while
	}// end run

}
