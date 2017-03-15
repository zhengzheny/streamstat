package com.gsta.bigdata.stream;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.counter.AbstractCounter;
import com.gsta.bigdata.stream.counter.Count;
import com.gsta.bigdata.stream.flush.IFlush;
import com.gsta.bigdata.stream.utils.Constants;

/**
 * 把计数器数据刷到redis中,但不删除计数器数据,主要针对按天统计情况
 * @author tianxq
 *
 */
public class ContinuousFlushCounterThread implements Runnable {
	private AbstractCounter counter;
	final Logger logger = LoggerFactory.getLogger(this.getClass());

	public ContinuousFlushCounterThread(AbstractCounter counter) {
		this.counter = counter;
	}

	@Override
	public void run() {
		while (true) {
			if (counter == null) {
				logger.error("flush counters is null");
				break;
			}

			//不设置的计数器,不做任何工作
			IFlush[] continuousFlushes = counter.getContinuousFlushes();
			if (continuousFlushes == null) break;

			//对于一天为单位的计数器，比如用户的一天流量，如果想看看当前的值，需要通过continuousFlushes到redis中，不删除计数器中的值
			for (Map.Entry<String, Count> mapEntry : counter.getCounters().entrySet()) {
				String key = mapEntry.getKey();

				// 如果没有定义key的field,则key直接为timestamp
				String timeStamp = key;
				// 如果计数器定义了key的field,key由field值和时间戳共同组成,在这里需要拆分
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
						// timestamp在key的最后一个字段
						timeStamp = values[i];
					}
				}
				Count count = mapEntry.getValue();

				for (IFlush flush : continuousFlushes) {
					flush.flush(counter.getName(), key, fieldValues, timeStamp,count.getCnt());
				}
			}

			//从配置中读取flush时间间隔
			try {
				Thread.sleep(counter.getFlushWaitTime() * 1000L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}// end while

	}

}
