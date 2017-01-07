package com.gsta.bigdata.stream.utils;

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

import com.google.common.base.Preconditions;

public class ConfigSingleton {
	private static ConfigSingleton singleton = new ConfigSingleton();
	private Map<String, Object> configs;

	@SuppressWarnings("unchecked")
	private ConfigSingleton() {
		URL url = Thread.currentThread().getContextClassLoader().getResource("config.yaml");
		//URL url = Thread.currentThread().getContextClassLoader().getResource("config-local.yaml");
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
	public Properties getKafkaProps() {
		Preconditions.checkNotNull(this.configs, "yaml config is null");

		Properties props = new Properties();

		Map<String, Object> kafkaConfig = (Map<String, Object>) this.configs.get("kafkaCluster");

		if (kafkaConfig != null) {
			props.put(StreamsConfig.APPLICATION_ID_CONFIG,kafkaConfig.get("app_id"));
			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaConfig.get("brokers"));
			props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,kafkaConfig.get("zookeeper"));
			props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
			props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
			props.put(StreamsConfig.CLIENT_ID_CONFIG,kafkaConfig.get("client_id"));
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,kafkaConfig.get("auto_offset_reset"));
			props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,kafkaConfig.get("timestamp_extractor"));
		}

		return props;
	}

	@SuppressWarnings("unchecked")
	public String getKafkaInputTopic() {
		Preconditions.checkNotNull(this.configs, "yaml config is null");
		Map<String, Object> kafkaConfig = (Map<String, Object>) this.configs.get("kafkaCluster");

		if (kafkaConfig != null) {
			return (String) kafkaConfig.get("topic");
		}

		return null;
	}

	@SuppressWarnings("unchecked")
	public List<String> getSourceFields() {
		Preconditions.checkNotNull(this.configs, "yaml config is null");
		
		Map<String, Object> sources = (Map<String, Object>) this.configs.get("sourceFields");
		if(sources != null){
			return (List<String>) sources.get("fields");
		}
		 
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public String getSourceDelimiter() {
		Preconditions.checkNotNull(this.configs, "yaml config is null");
		
		Map<String, Object> sources = (Map<String, Object>) this.configs.get("sourceFields");
		if(sources != null){
			return (String) sources.get("delimiter");
		}
		 
		return null;
	}

	@SuppressWarnings("unchecked")
	public List<String> getCounterList() {
		Preconditions.checkNotNull(this.configs, "yaml config is null");
		
		return (List<String>) this.configs.get("couterList");
	}
	
	@SuppressWarnings("unchecked")
	public String[] getCounterFlushes(String counterName) {
		Preconditions.checkNotNull(this.configs, "yaml config is null");

		Map<String, Object> counters = (Map<String, Object>) this.configs.get(counterName);
		if (counters != null) {
			String flushes = (String)counters.get("flushes");
			if(flushes != null){
				return flushes.split(",",-1);
			}
		}
		
		//default is console
		String[] ret = new String[1];
		ret[0] = Constants.FLUSH_CONSOLE;
		return ret;
	}
	
	@SuppressWarnings("unchecked")
	public String[] getCounterContinuousFlushes(String counterName) {
		Preconditions.checkNotNull(this.configs, "yaml config is null");

		Map<String, Object> counters = (Map<String, Object>) this.configs.get(counterName);
		if (counters != null) {
			String flushes = (String)counters.get("continuousFlushes");
			if(flushes != null){
				return flushes.split(",",-1);
			}
		}
		
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public double getCounterFlushTimeGapRatio(String counterName){
		Preconditions.checkNotNull(this.configs, "yaml config is null");

		Map<String, Object> counters = (Map<String, Object>) this.configs.get(counterName);
		if (counters != null) {
			double ret =  (double)counters.getOrDefault("flushTimeGapRatio",1.0);
			if(ret >= 1.0){
				return ret;
			}
		}
		
		return 1.0;
	}
	
	@SuppressWarnings("unchecked")
	public int getCounterFlushWaitTime(String counterName){
		Preconditions.checkNotNull(this.configs, "yaml config is null");

		Map<String, Object> counters = (Map<String, Object>) this.configs.get(counterName);
		if (counters != null) {
			return (int)counters.getOrDefault("flushWaitTime", 60);
		}
		
		return 60;
	}
	
	@SuppressWarnings("unchecked")
	public String getCounterStatField(String counterName){
		Preconditions.checkNotNull(this.configs, "yaml config is null");

		Map<String, Object> counters = (Map<String, Object>) this.configs.get(counterName);
		if (counters != null) {
			return (String)counters.get("keyFields");
		}
		
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public String getCounterType(String counterName){
		Preconditions.checkNotNull(this.configs, "yaml config is null");

		Map<String, Object> counters = (Map<String, Object>) this.configs.get(counterName);
		if (counters != null) {
			return (String)counters.get("type");
		}
		
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public String getCounterBloomFilter(String counterName){
		Preconditions.checkNotNull(this.configs, "yaml config is null");

		Map<String, Object> counters = (Map<String, Object>) this.configs.get(counterName);
		if (counters != null) {
			return (String)counters.get("bloomFilter");
		}
		
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, Object> getCounterConf(String counterName){
		Preconditions.checkNotNull(this.configs, "yaml config is null");

		return (Map<String, Object>) this.configs.get(counterName);
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> getBloomFilter(){
		Preconditions.checkNotNull(this.configs, "yaml config is null");

		return (Map<String, Object>) this.configs.get("bloomFilter");
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> getElasticsearchConf() {
		Preconditions.checkNotNull(this.configs, "yaml config is null");

		return (Map<String, Object>) this.configs.get("elasticsearch");
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> getRedisProperties() {
		Preconditions.checkNotNull(this.configs, "yaml config is null");
		return (Map<String, Object>) this.configs.get("redis");
	}

	public static ConfigSingleton getInstance() {
		return singleton;
	}
	
	public static void main(String[] args){
		System.out.println(ConfigSingleton.getInstance().getSourceFields());
		System.out.println(ConfigSingleton.getInstance().getSourceDelimiter());
	}
}
