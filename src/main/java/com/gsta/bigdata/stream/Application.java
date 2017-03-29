package com.gsta.bigdata.stream;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.counter.AbstractCounter;
import com.gsta.bigdata.stream.counter.Count;
import com.gsta.bigdata.stream.flush.IFlush;
import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;

/**
 * 启动kafak stream,读取数据源的消息进行计数器处理,并把结果写入到kafak topic中
 * 第一次计数按照mdn号码来做区分,得到配置文件中的计数器,整体程序：
 * 1.flume把dpi数据源写入到kafka 4GDPI topic
 * 2.Application 从4GDPI topic读取，并计算每一个计数器情况,计算完后写入到对应的kafka topic,在counter配置文件中配置outputKafkaTopic,默认写入到defaultOutputTopic
 * 3.GroupbyCounterApp把outputKafkaTopic作为输入，进行汇总,并写入到kafka中
 * 4.logstash把汇总后的结果写入到Elasticsearch
 * @author tianxq
 *
 */
public class Application {
	final static Logger logger = LoggerFactory.getLogger(Application.class);
	
	private static Map<String,AbstractCounter> getCounters(){
		List<String> lstCounter = ConfigSingleton.getInstance().getCounterList();
		if(lstCounter == null){
			logger.error("there is no counter in config.yaml...");
			System.exit(-1);
		}
		
		Map<String,AbstractCounter> counters = new HashMap<String,AbstractCounter>();
		for(int i = 0 ;i<lstCounter.size();i++){
			String name = lstCounter.get(i);
			AbstractCounter counter = CounterFactory.createCounter(name);
			counters.put(name, counter);
		}
		
		return counters;
	}
	
	//关闭程序时,把结果数据写到kafak
	private static void flushCounters(Map<String,AbstractCounter> counters) {
		if (counters == null) return;

		for (Map.Entry<String, AbstractCounter> entry:counters.entrySet()) {
			AbstractCounter counter = entry.getValue();
			if (counter == null) continue;

			logger.info("begin flush counter {}",counter.getName());

			for (Map.Entry<String, Count> mapEntry : counter.getCounters().entrySet()) {
				String key = mapEntry.getKey();
				if (key == null) continue;

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
				if(count.getCnt() > 0){
					for (IFlush flush : counter.getFlushes()) {
						flush.flush(counter.getName(), key,fieldValues, timeStamp,count.getCnt());
					}
					counter.getCounters().remove(key);
					counter.getCountersTimeStamp().remove(key);
				}
			}
		}// end for counters
		logger.info("finishing flush all counters");
	}
	
	/**
	 * 把原始数据解析成key-value
	 * @param value  -原始数据记录
	 * @param sourceFields  -数据源字段定义
	 * @param sourceDelimiter -数据源字段间的分隔符
	 * @return
	 */
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
			logger.error("source field count={} ,but source definition count={}",
					fields.length, sourceFields.size());
	}

		return data;
	}
	
	public static void main(String[] args) {
		if(args.length < 2){
			//由于和GroupbyCounterApp共享配置文件,application.id难以做到统一,由命令行输入
			System.out.println("usage:" + Application.class.getSimpleName() + 
					" configFile application.id");
			System.exit(-1);
		}
		
		ConfigSingleton.getInstance().init(args[0]);
		//定义成map,在kafkawriter线程中会用到counter对象内容
		Map<String,AbstractCounter> counters = getCounters();
		List<String> sourceFields = ConfigSingleton.getInstance().getSourceFields();
		String sourceDelimiter = ConfigSingleton.getInstance().getSourceDelimiter();	
		
		Properties props = ConfigSingleton.getInstance().getKafkaProps();
		String topic = props.getProperty("inputTopic");
		String applicationId = args[1];
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
		logger.info("kafka topic:{}" ,topic);
		logger.info("kafka config:\n{}",props);
		
		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> source = builder.stream(topic);
		
		//初始化布隆过滤器,全局一套,各个counter共用
		BloomFilterFactory.getInstance().init();

		source.map(new KeyValueMapper<String, String, KeyValue<String, String>>() {			
			@Override
			public KeyValue<String, String> apply(String key, String value) {
				logger.debug("key=" + key + ",value=" + value);
				Map<String, String> data = parseValue(value,sourceFields,sourceDelimiter);
				
				//deal with by bloom filter
				String mdn = data.get(Constants.FIELD_MSISDN);
				long timeStamp = -1L;
				try {
					timeStamp = Long.parseLong(data.get(Constants.FIELD_TIMESTAMP));
				} catch (NumberFormatException e) {
					logger.error("invalid timestamp field:{}",e.getMessage());
					return new KeyValue<>(null, null);
				}
								
				//每一个计数器处理计数
				for(Map.Entry<String, AbstractCounter> entry:counters.entrySet()){
					entry.getValue().add(key, data,mdn,timeStamp);
				}
				
				//布隆过滤插入数据
				BloomFilterFactory.getInstance().add(timeStamp, data);
				return new KeyValue<>(null, null);
			}
		});

		final KafkaStreams streams = new KafkaStreams(builder, props);
		streams.start();

		//每一个计数器都有一个线程去检查统计周期是否完成
		Thread[] counterFlushthreads = new Thread[counters.size()];
		//continuousFlushCounterThreads是有些计数器统计周期很长,中间刷新到redis,供查询
		Thread[] continuousFlushCounterThreads = new Thread[counters.size()];
		int i = 0;
		for(Map.Entry<String, AbstractCounter> entry:counters.entrySet()){
			counterFlushthreads[i] = new Thread(new FlushCounterThread(entry.getValue()));
			counterFlushthreads[i].start();
			
			continuousFlushCounterThreads[i] = new Thread(new ContinuousFlushCounterThread(entry.getValue()));
			continuousFlushCounterThreads[i].start();
			i++;
		}
		
		//写kafka线程，flushThread线程把数据写入到发送队列,kafka线程读取队列中元素
		//ESWriterThread esWriter = new ESWriterThread();
		KafkaWriterThread writer = new KafkaWriterThread(counters);
		Thread  writerThread = new Thread(writer);
		writerThread.start();
		
		//关闭服务器时,把缓冲区数据处理掉
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					logger.info("The JVM Hook is execute...");
					//close kafka stream
					streams.close();
					
					for(int i=0;i<counters.size();i++){
						counterFlushthreads[i].interrupt();
						counterFlushthreads[i].join(1000);
					}
					logger.info("stop flush result thread...");
					
					//flush counter result
					flushCounters(counters);
					
					//close flush
					for(Map.Entry<String, AbstractCounter> entry:counters.entrySet()){
						for(IFlush flush:entry.getValue().getFlushes()){
							flush.close();
						}
					}
					
					writerThread.join(1000);
					writer.close();
					
					logger.info("kafka stream agent stoped...");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});//end shutdown hook
	}
}
