package com.gsta.bigdata.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import net.sf.json.JSONObject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;

public class KafkaWriterThread implements Runnable {
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	private final KafkaProducer<String, String> producer;
	private final String topic;

	public KafkaWriterThread() {
		Properties props = ConfigSingleton.getInstance().getKafkaProps();
		this.producer = new KafkaProducer<>(props);
		this.topic = ConfigSingleton.getInstance().getKafkaOutputTopic();
	}

	@Override
	public void run() {
		while (true) {
			Map<String, HashMap<String, Object>> requests = CounterCacheSingleton.getSingleton().getRequests();
			int counterSize = requests.size();
			int flushCount = 0;

			for (Map.Entry<String, HashMap<String, Object>> mapEntry : requests.entrySet()) {
				String reqKey = mapEntry.getKey();
				HashMap<String, Object> map = mapEntry.getValue();

				JSONObject jsonObject = JSONObject.fromObject(map);
				String message = jsonObject.toString();
				try {
					producer.send(
							new ProducerRecord<>(topic, reqKey, message)).get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}

				requests.remove(reqKey);
				flushCount++;
			}

			logger.info("kafka requests size=" + counterSize + ",writeCount=" + flushCount);
			try {
				Thread.sleep(20 * 1000L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void close() {
		this.producer.close();
	}
}
