package com.gsta.bigdata.stream;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.SysUtils;

public class FlushCounterResult implements Runnable {
	private AbstractCounter counter;
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	private int processId;

	public FlushCounterResult(AbstractCounter counter) {
		this.counter = counter;
		this.processId = SysUtils.getProcessID();
	}

	@Override
	public void run() {
		while (true) {
			if (counter == null) {
				logger.error("flush counters is null");
				break;
			}
			
			//logger.info("begin flush counter:" + counter.getName());
			Map<String,Map<String, Count>> results = counter.getCounters();
			if(results == null){
				logger.error(counter.getName() + "'s result is null");
				break;
			}
			
			for(Map<String,Count> result:results.values()){
				for (Map.Entry<String, Count> mapEntry : result.entrySet()) {
					String key = mapEntry.getKey();
					if(key == null) continue;
					
					String keyField = null,timeStamp=null;
					int idx = key.indexOf(Constants.KEY_DELIMITER);
					if(idx > 0){
						keyField = key.substring(0, idx);
						timeStamp = key.substring(idx+1);
					}else{
						timeStamp = key;
					}
					
					Count count = mapEntry.getValue();
					//after flush time gap of counter,flush to disk and remove it from memory
					long deltaTime = System.currentTimeMillis() - count.getTimestamp();
					if (count.isFinished() || deltaTime > counter.getFlushTimeGap()) {
						for(IFlush flush:counter.getFlushes()){
							flush.flush(counter.getName(),keyField, timeStamp,count.getCnt(), processId);
						}
						result.remove(key);
					}
					
					/*sometimes the time gap of counter maybe is the whole day,
					the user would like to see the middle result,system flush to disk,
					general flush includes redis and console
					*/
					IFlush[] continuousFlushes = counter.getContinuousFlushes();
					if(continuousFlushes != null){
						for(IFlush flush:continuousFlushes){
							flush.flush(counter.getName(),keyField, timeStamp,count.getCnt(), processId);
						}
					}
				}//end for result,map.values()
			}//end for results

			//sleep flushWaitTime second
			try {
				Thread.sleep(counter.getFlushWaitTime() * 1000L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}//end while
	}//end run
}
