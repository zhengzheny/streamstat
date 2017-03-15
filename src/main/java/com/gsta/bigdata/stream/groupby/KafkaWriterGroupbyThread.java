package com.gsta.bigdata.stream.groupby;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.gsta.bigdata.stream.CounterCacheSingleton;
import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;

public class KafkaWriterGroupbyThread implements Runnable {
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	private final KafkaProducer<String, String> producer;
	private int kafkaWriterThreadWaitTime;
	private String outputTopic;

	public KafkaWriterGroupbyThread(String outputTopic) {
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
		
		this.outputTopic = outputTopic;
	}

	@Override
	public void run() {
		int flushCount = 1;
		Gson gson = new Gson();
		
		while (true) {
			// 从队列中读取
			GroupbyCount groupbyCount = (GroupbyCount)CounterCacheSingleton.getSingleton().poll();
			if (groupbyCount != null) {
				Map<String,Object> map = new HashMap<String,Object>();
				map.putAll(groupbyCount.getJsonCount().getMap());
				map.put(Constants.OUTPUT_FIELD_COUNT, groupbyCount.getJsonCount().getCount());
				//为了测试,打印累积的进程个数id
				//map.put(Constants.OUTPUT_STREAM_NO, groupbyCount.getCnt());
				//key不写入到es中
				map.remove(Constants.OUTPUT_FIELD_KEY);
				String message = gson.toJson(map);
				producer.send(new ProducerRecord<>(outputTopic,message));

				flushCount++;
			} else {
				try {
					Thread.sleep(kafkaWriterThreadWaitTime * 1000L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			if (flushCount % 10000 == 0)
				logger.info("write {} counter to kafka,the queue size={}", flushCount,
						CounterCacheSingleton.getSingleton().getPoolSize());

		}
	}

	public void close() {
		producer.close();
	}
}
