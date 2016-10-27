package com.gsta.bigdata.stream.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.yaml.snakeyaml.Yaml;

public class ConfigSingleton {
	private static ConfigSingleton singleton = null;  
	private Map<String, Object> configs;
	
	@SuppressWarnings("unchecked")
	private ConfigSingleton() {
		URL url = Thread.currentThread().getContextClassLoader().getResource("config.yaml");
		if (url != null) {
			try {
				InputStream is = new FileInputStream(url.getPath());
				if (is != null) {
					Yaml yaml = new Yaml();
					this.configs = (Map<String, Object>) yaml.load(is);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public Properties getKafkaProps(){
		Properties props = new Properties();
		if(this.configs != null){
			Map<String, Object> kafkaConfig = (Map<String, Object>) this.configs.get("kafkaCluster");
			
			if(kafkaConfig != null){
				props.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaConfig.get("app_id"));
				props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.get("brokers"));
				props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, kafkaConfig.get("zookeeper"));
				props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
				props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
				props.put(StreamsConfig.CLIENT_ID_CONFIG, kafkaConfig.get("client_id"));
				props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConfig.get("auto_offset_reset"));
				props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, kafkaConfig.get("timestamp_extractor"));
			}
		}
		
		return props;
	}
	
	@SuppressWarnings("unchecked")
	public String getKafkaInputTopic(){
		if(this.configs != null){
			Map<String, Object> kafkaConfig = (Map<String, Object>) this.configs.get("kafkaCluster");
			
			if(kafkaConfig != null){
				return (String)kafkaConfig.get("topic");
			}
		}
		
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public List<String> getSourceFields(){
		if(this.configs != null){
			return (List<String>)this.configs.get("sourceFields");
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public List<String> getCounterList(){
		if(this.configs != null){
			return (List<String>)this.configs.get("couterList");
		}
		return null;
	}
	
	public synchronized static ConfigSingleton getInstance(){
		if(singleton == null){
			singleton = new ConfigSingleton();
		}
		return singleton;		
	}
	
	
	public static void main(String[] args) throws FileNotFoundException {
		System.out.println(ConfigSingleton.getInstance().getKafkaProps());	
		System.out.println(ConfigSingleton.getInstance().getKafkaInputTopic());	
		System.out.println(ConfigSingleton.getInstance().getSourceFields());	
		System.out.println(ConfigSingleton.getInstance().getCounterList());		
	}
}
