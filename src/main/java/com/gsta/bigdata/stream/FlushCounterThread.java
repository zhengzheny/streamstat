package com.gsta.bigdata.stream;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.counter.AbstractCounter;
import com.gsta.bigdata.stream.counter.Count;
import com.gsta.bigdata.stream.counter.CountTimeStamp;
import com.gsta.bigdata.stream.flush.IFlush;
import com.gsta.bigdata.stream.utils.Constants;

/**
 * 把计数器的
 * @author tianxq
 *
 */
public class FlushCounterThread implements Runnable {
	private AbstractCounter counter;
	final Logger logger = LoggerFactory.getLogger(this.getClass());

	public FlushCounterThread(AbstractCounter counter) {
		this.counter = counter;
	}

	@Override
	public void run() {
		while (true) {
			if (counter == null) {
				logger.error("flush counters is null");
				break;
			}

			int flushCount = 0;
			int counterSize = counter.getCountersTimeStamp().size();
			// logger.info("begin flush counter:" + counter.getName());
			boolean flag = false;
			// 扫描CountersTimeStamp,发现超时,将会从Counters中获取count进行flush,并删除count
			for (Map.Entry<String, CountTimeStamp> mapEntry : counter.getCountersTimeStamp().entrySet()) {
				String key = mapEntry.getKey();
				if (key == null)  continue;

				CountTimeStamp countTimeStamp = mapEntry.getValue();
				//当前系统时间和计数器的创建时间的差超过设定的阀值，flush出去并从集合中删除
				long deltaTime = System.currentTimeMillis() - countTimeStamp.getTimestamp();
				if (deltaTime >= counter.getFlushTimeGap()) {
					//如果没有定义key的field,则key直接为timestamp
					String timeStamp = key;
					//如果计数器定义了key的field,key由field值和时间戳共同组成,在这里需要拆分
					Map<String, String> fieldValues = new HashMap<String, String>();
					if (counter.getKeyFields() != null) {
						String[] values = key.split(Constants.KEY_DELIMITER, -1);
						if (values != null
								&& values.length - counter.getKeyFields().length == 1) {
							int i = 0;
							for (String field : counter.getKeyFields()) {
								fieldValues.put(field, values[i]);
								i++;
							}
							//timestamp在key的最后一个字段
							timeStamp = values[i];
						}
					}
					Count count = counter.getCounters().get(key);
					for (IFlush flush : counter.getFlushes()) {
						flush.flush(counter.getName(), key, fieldValues,timeStamp, count.getCnt());
					}
//				continue flush
					IFlush[] continuousFlushes = counter.getContinuousFlushes();
					if (continuousFlushes == null) {
						counter.getCounters().remove(key);
						counter.getCountersTimeStamp().remove(key);
						flushCount++;
						break;
					}
					for (IFlush flush : continuousFlushes) {
//						logger.info("jiancha"+counter.getName()+key+fieldValues+timeStamp);
						flush.flush(counter.getName(), key, fieldValues,timeStamp, count.getCnt());						
					}
					//flush出去后,不删除计数器，continue去删
					counter.getCounters().remove(key);
					counter.getCountersTimeStamp().remove(key);
					flushCount++;
				}
				flag=true;
			}// end for results

			if (flag) {
				logger.info("flush {} counterSize={},flushCount={}" ,counter.getName(),counterSize,flushCount);
			}
			try {
				Thread.sleep(500L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}// end while
	}// end run

}
