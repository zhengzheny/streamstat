package com.gsta.bigdata.stream;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.SysUtils;

public class FlushCounterResult implements Runnable {
	private AbstractCounter counter;
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	private int processId;
	private String ip;

	public FlushCounterResult(AbstractCounter counter) {
		this.counter = counter;
		
		this.processId = SysUtils.getProcessID() ;
		this.ip = SysUtils.getLastIp();
	}

	@Override
	public void run() {
		while (true) {
			if (counter == null) {
				logger.error("flush counters is null");
				break;
			}
			
			int flushCount = 0;
			int counterSize = counter.getCounters().size();
			// logger.info("begin flush counter:" + counter.getName());
			for (Map.Entry<String, Count> mapEntry : counter.getCounters().entrySet()) {
				String key = mapEntry.getKey();
				if (key == null)
					continue;

				String timeStamp = null;
				Map<String,String> fieldValues = new HashMap<String, String>();
				if(counter.getKeyFields() == null){
					timeStamp = key;
				}else{
					String[] values = key.split(Constants.KEY_DELIMITER, -1);
					if (values != null
							&& values.length - counter.getKeyFields().length == 1) {
						int i = 0;
						for(String field:counter.getKeyFields()){
							fieldValues.put(field, values[i]);
							i++;
						}
						timeStamp = values[i];
					}
				}
				
				Count count = mapEntry.getValue();
				// after flush time gap of counter,flush to disk and remove it from memory
				long deltaTime = System.currentTimeMillis() - count.getTimestamp();
				if (count.getCnt() >0 && deltaTime > counter.getFlushTimeGap()) {
					for (IFlush flush : counter.getFlushes()) {
						flush.flush(counter.getName(), key,fieldValues, timeStamp,
								count.getCnt(), processId,ip);
					}
					counter.getCounters().remove(key);
					flushCount++;
				}

				/*
				 * sometimes the time gap of counter maybe is the whole day, the
				 * user would like to see the middle result,system flush to
				 * disk, general flush includes redis and console
				 */
				IFlush[] continuousFlushes = counter.getContinuousFlushes();
				if (continuousFlushes != null) {
					for (IFlush flush : continuousFlushes) {
						flush.flush(counter.getName(),key, fieldValues, timeStamp,
								count.getCnt(), processId,ip);
					}
				}
			}// end for results

			logger.info("flush " + counter.getName() + " counterSize="
					+ counterSize + ",flushCount=" + flushCount);
			// sleep flushWaitTime second
			try {
				Thread.sleep(counter.getFlushWaitTime() * 1000L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}// end while
	}// end run
	
}
