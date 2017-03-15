package com.gsta.bigdata.stream;

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

import com.gsta.bigdata.stream.groupby.FlushGroupbyThread;
import com.gsta.bigdata.stream.groupby.GroupbyCount;
import com.gsta.bigdata.stream.groupby.GroupbyCounter;
import com.gsta.bigdata.stream.groupby.KafkaWriterGroupbyThread;
import com.gsta.bigdata.stream.utils.ConfigSingleton;

/**
 * 启动kafak stream,把Application处理的结果进行汇总,并写入到最终的kafka topic,由logstash写入到elasticsearch
 * @author tianxq
 *
 */
public class GroupbyCounterApp {
	final static Logger logger = LoggerFactory.getLogger(GroupbyCounterApp.class);
	
	public static void main(String[] args) {
		if(args.length < 6){
			System.out.println("usage:" + GroupbyCounterApp.class.getSimpleName() 
					+ " configFile application.id inputTopic outputTopic streamAgentNum flushTime"
					+"\n configFile- configure file"
					+"\n application.id -kafka stream application id"
					+"\n inputTopic - kafka input topic"
					+"\n outputTopic - kafka output topic"
					+"\n streamAgentNum - kafka stream counter process count"
					+"\n flushTime - counter result flush time,unit is second");
			System.exit(-1);
		}
		
		ConfigSingleton.getInstance().init(args[0]);
		//kafka stream计算出的counter可能分成多个临时性的
		String applicationId = args[1];
		String intputTopic = args[2];
		String outputTopic = args[3];
		int streamAgentNum = Integer.parseInt(args[4]);
		int flushTime = Integer.parseInt(args[5]);
		
		Properties props = ConfigSingleton.getInstance().getKafkaProps();
		//一台机器启动多个stream进程，application.id需要不一样
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
		logger.info("input topic={},outputTopic={}",intputTopic,outputTopic);
		logger.info("kafka config:\n" + props);
		
		GroupbyCounter groupbyCounter = new GroupbyCounter(streamAgentNum);
		
		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> source = builder.stream(intputTopic);
		source.map(new KeyValueMapper<String, String, KeyValue<String, String>>() {			
			@Override
			public KeyValue<String, String> apply(String key, String value) {
				//计数器进行汇总
				groupbyCounter.groupby(value);
				
				return new KeyValue<>(null, null);
			}
		});

		final KafkaStreams streams = new KafkaStreams(builder, props);
		streams.start();
		
		//对于进程数不够的,超时后,写到队列中
		Thread flushThread = new Thread(new FlushGroupbyThread(groupbyCounter,flushTime));
		flushThread.start();
		//从队列中读取数据写kafka
		Thread kafkaThread = new Thread(new KafkaWriterGroupbyThread(outputTopic));
		kafkaThread.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					logger.info("The JVM Hook is execute...");
					//close kafka stream
					streams.close();
					
					flushThread.interrupt();
					flushThread.join(1000);
					
					for (Map.Entry<String, GroupbyCount> entry:groupbyCounter.getCounters().entrySet()) {
						GroupbyCount groupbyCount = entry.getValue();
						CounterCacheSingleton.getSingleton().offer(groupbyCount);
						groupbyCounter.getCounters().remove(entry.getKey());
					}
					
					kafkaThread.interrupt();
					kafkaThread.join(1000);
			
					logger.info("kafka stream agent stoped...");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});//end shutdown hook
	}
}
