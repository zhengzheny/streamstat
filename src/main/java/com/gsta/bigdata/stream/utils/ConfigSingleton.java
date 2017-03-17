package com.gsta.bigdata.stream.utils;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.yaml.snakeyaml.Yaml;

import com.google.common.base.Preconditions;

public class ConfigSingleton {
	private static ConfigSingleton singleton = new ConfigSingleton();
	private Map<String, Object> configs;

	private ConfigSingleton() {
		/*URL url = Thread.currentThread().getContextClassLoader().getResource("config.yaml");
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
		}*/
	}
	
	@SuppressWarnings("unchecked")
	public void init(String configFile){
		try {
			InputStream inputStream =  FileUtils.getInputFile(configFile);
			if (inputStream != null) {
				Yaml yaml = new Yaml();
				this.configs = (Map<String, Object>) yaml.load(inputStream);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
			
	}

	@SuppressWarnings("unchecked")
	public Properties getKafkaProps() {
		Preconditions.checkNotNull(this.configs, "yaml config is null");

		Properties props = new Properties();

		Map<String, String> kafkaConfig = (Map<String, String>) this.configs.get("kafkaCluster");
		for(Map.Entry<String, String> entry:kafkaConfig.entrySet()){
			props.put(entry.getKey(), entry.getValue());
			
			props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
			props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		}
		
		return props;
	}
	
	@SuppressWarnings("unchecked")
	public String getDefaultKafkaOutputTopic() {
		Preconditions.checkNotNull(this.configs, "yaml config is null");
		Map<String, Object> kafkaConfig = (Map<String, Object>) this.configs.get("kafkaCluster");

		if (kafkaConfig != null) {
			return (String) kafkaConfig.get("defaultOutputTopic");
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
	public String getCounterType(String counterName){
		Preconditions.checkNotNull(this.configs, "yaml config is null");

		Map<String, Object> counters = (Map<String, Object>) this.configs.get(counterName);
		if (counters != null) {
			return (String)counters.get("type");
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
}
