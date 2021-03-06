package com.gsta.bigdata.stream;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.gsta.bigdata.stream.counter.AbstractCounter;
import com.gsta.bigdata.stream.utils.ConfigSingleton;

public class KafkaWriterThread implements Runnable {
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	private final KafkaProducer<String, String> producer;
	private int kafkaWriterThreadWaitTime;
	private Map<String,AbstractCounter> counters;

	public KafkaWriterThread(Map<String,AbstractCounter> counters) {
		Properties props = ConfigSingleton.getInstance().getKafkaProps();

		Properties producerProps = new Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,props.get("key.serializer"));
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,props.get("value.serializer"));
		producerProps.put(ProducerConfig.ACKS_CONFIG,props.get(ProducerConfig.ACKS_CONFIG));
		producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG,props.get(ProducerConfig.BATCH_SIZE_CONFIG));
		producerProps.put(ProducerConfig.LINGER_MS_CONFIG,props.get(ProducerConfig.LINGER_MS_CONFIG));

		this.producer = new KafkaProducer<String, String>(producerProps);
		this.kafkaWriterThreadWaitTime = (int) props.getOrDefault("kafkaWriterThreadWaitTime", 1);
		
		this.counters = counters;
	}

	@Override
	public void run() {
		//计数器从1开始,避免没有数据之前,大量打印0的日志
		int flushCount = 1;
		Gson gson = new Gson();

		while (true) {
			// 从队列中读取
			CounterCount jsonCount = (CounterCount)CounterCacheSingleton.getSingleton().poll();
			if (jsonCount != null) {
				String counterName = jsonCount.getCounterName();
				AbstractCounter counter = this.counters.get(counterName);
				//如果counter已经定义topic,就用counter的topic,如果没有就用默认输出
				String topic = counter.getOutputTopic();
				
				//消息的分区采用keyFields的第一个字段,如CGI,domain等,没有keyFields,为空字符串,分在同一个分区
				String key = "";
				String[] keyFields = counter.getKeyFields();
				if(keyFields != null){
					key = jsonCount.getValue(keyFields[0]);
				}
				
				String message = gson.toJson(jsonCount);
				producer.send(new ProducerRecord<>(topic,key,message));

				flushCount++;
			} else {
				try {
					Thread.sleep(kafkaWriterThreadWaitTime * 1000L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			if (flushCount % 10000 == 0){
				int size = CounterCacheSingleton.getSingleton().getPoolSize();
				logger.info("write {} counter to kafka,the queue size={}", flushCount,size);
			}
		}
	}

	public void close() {
		producer.close();
	}
}
