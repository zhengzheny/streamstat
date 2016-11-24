package com.gsta.bigdata.stream;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.MySQLConnSingleton;
import com.gsta.bigdata.stream.utils.SysUtils;

public class Application {
	final static Logger logger = LoggerFactory.getLogger(Application.class);
	
	private static AbstractCounter[] getCounters(){
		List<String> lstCounter = ConfigSingleton.getInstance().getCounterList();
		if(lstCounter == null){
			logger.error("there is no counter in config.yaml...");
			System.exit(-1);
		}
		
		AbstractCounter[] counters = new AbstractCounter[lstCounter.size()];
		for(int i = 0 ;i<lstCounter.size();i++){
			counters[i] = CounterFactory.createCounter(lstCounter.get(i));
		}
		
		return counters;
	}
	
	//shutdown the application,flush counters 
	private static void flushCounters(AbstractCounter[] counters){
		if(counters == null){
			return;
		}
		
		int processId = SysUtils.getProcessID();
		
		for (AbstractCounter counter : counters) {
			if(counter == null) continue;
			
			logger.info("begin flush counter:" + counter.getName());
			Map<String,Map<String, Count>> results = counter.getCounters();
			if(results == null) continue;
			
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
						results.remove(key);
					}
				}//end for result,map.values()
			}//end for results
		}//end for counters
		logger.info("finishing flush all counters");
	}
	
	private static Map<String, String> parseValue(String value,
			List<String> sourceFields, String sourceDelimiter) {
		if(value == null || sourceFields == null || sourceDelimiter == null){
			return null;
		}
		
		Map<String, String> data = new HashMap<String, String>();
		String[] fields = value.split(sourceDelimiter, -1);
		if (fields.length == sourceFields.size()) {
			for (int i = 0; i < fields.length; i++) {
				data.put(sourceFields.get(i), fields[i]);
			}
		} else {
			logger.error("source field count=" + fields.length
					+ ",but source definition count=" + sourceFields.size());
		}

		return data;
	}
	
	public static void main(String[] args) {
		AbstractCounter[] counters = getCounters();
		
		List<String> sourceFields = ConfigSingleton.getInstance().getSourceFields();
		String sourceDelimiter = ConfigSingleton.getInstance().getSourceDelimiter();	
		
		String topic = ConfigSingleton.getInstance().getKafkaInputTopic();
		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> source = builder.stream(topic);

		source.map(new KeyValueMapper<String, String, KeyValue<String, String>>() {			
			@Override
			public KeyValue<String, String> apply(String key, String value) {
				logger.debug("key=" + key + ",value=" + value);
				Map<String, String> data = parseValue(value,sourceFields,sourceDelimiter);
				for(AbstractCounter counter:counters){
					if(counter != null) counter.add(key, data);
				}
				return new KeyValue<>(null, null);
			}
		});

		Properties props = ConfigSingleton.getInstance().getKafkaProps();
		logger.info("kafka topic:" + topic);
		logger.info("kafka config:\n" + props);
		final KafkaStreams streams = new KafkaStreams(builder, props);
		streams.start();

		//every counter has one thread to flush
		for(int i=0;i<counters.length;i++){
			new Thread(new FlushCounterResult(counters[i])).start();
		}
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@SuppressWarnings("static-access")
			public void run() {
				try {
					logger.info("The JVM Hook is execute...");
					
					//flush counter result
					flushCounters(counters);
					
					//close flush
					for(AbstractCounter counter:counters){
						for(IFlush flush:counter.getFlushes()){
							flush.close();
						}
					}
					
					//close mysql pool
					MySQLConnSingleton.getInstance().close();
					
					//close kafka stream
					streams.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});//end shutdown hook
	}
}
