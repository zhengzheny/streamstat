package com.gsta.bigdata.stream;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.config.ConfigSingleton;

public class Application {
	
	public static void main(String[] args) {
		final Logger logger = LoggerFactory.getLogger(Application.class);
		
		String topic = ConfigSingleton.getInstance().getKafkaInputTopic();
		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> source = builder.stream(topic);
		source.map(new KeyValueMapper<String,String,KeyValue<String, String>>(){
			@Override
			public KeyValue<String, String> apply(String key, String value) {
				System.out.println("key="+key+",value="+value);
				return new KeyValue<>(key, value);
			}
		});
		
		Properties props = ConfigSingleton.getInstance().getKafkaProps();
		logger.info("kafka config:\n" + props);
		final KafkaStreams streams = new KafkaStreams(builder, props);
		streams.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					logger.info("The JVM Hook is execute...");
					streams.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
}
