package com.gsta.bigdata.stream;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;

public class ElasticsearchFlush implements IFlush {
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	private TransportClient client;
	private final static String CLUSTER_NAME = "cluster.name";
	private final static String SNIFF = "client.transport.sniff";
	private String indexName;
	private final static String DAY_DELI = "-";
	private final static String HOUR_DELI = ":";
	
	@SuppressWarnings("unchecked")
	public ElasticsearchFlush() {
		String clusterName = (String) ConfigSingleton.getInstance()
				.getElasticsearchConf().get(CLUSTER_NAME);
		boolean sniff = (boolean) ConfigSingleton.getInstance()
				.getElasticsearchConf().get(SNIFF);

		Settings settings = Settings.builder().put(CLUSTER_NAME, clusterName)
				.put(SNIFF, sniff).build();

		this.client = new PreBuiltTransportClient(settings);
		int port = (int) ConfigSingleton.getInstance().getElasticsearchConf().get("port");
		List<String> servers = (List<String>) ConfigSingleton.getInstance()
				.getElasticsearchConf().get("cluster.nodes");
		for (String server : servers) {
			try {
				this.client.addTransportAddress(new InetSocketTransportAddress(
						InetAddress.getByName(server), port));
			} catch (UnknownHostException e) {
				logger.error("init elasticsearch occur error:" + e.toString());
			}
		}// end for
		
		this.indexName = (String) ConfigSingleton.getInstance()
				.getElasticsearchConf().get("index.name");
	}
	
	@Override
	public void flush(String counterName,String key, Map<String, String> fieldValues, String timeStamp,
			long count, int processId) {
		Map<String,Object> map = new HashMap<String,Object>();
		
		if(fieldValues != null && fieldValues.size() > 0)  {
			map.putAll(fieldValues);
		}
		map.put("timeStamp",this.formatTimestamp(timeStamp));
		map.put("count", count);
		map.put("processId", processId);
		map.put("counterName", counterName);
		String tempKey = key + Constants.KEY_DELIMITER + processId;
		
		IndexResponse response = client
				.prepareIndex(this.indexName, counterName, tempKey)
				.setSource(map).get();
		if (response.status() == RestStatus.CREATED) {
			logger.info("counterName=" + counterName +
					",keyFields=" + fieldValues.toString() +
					",timeStamp=" + timeStamp + 
					",count=" + count + 
					",processId=" + processId + 
					" create index success...");
		}else{
			logger.warn(response.toString());
		}
	}

	@Override
	public void close() {
		this.client.close();
	}
	
	public String formatTimestamp(String key) {
		if (key != null) {
			String ret = null;
			switch (key.length()) {
			case 8:
				// yyyyMMdd
				ret = key.substring(0, 4) + DAY_DELI + key.substring(4, 6)
						+ DAY_DELI + key.substring(6, 8) + "T00:00:00+0800";
				break;
			case 10:
				// yyyyMMddHH
				ret = key.substring(0, 4) + DAY_DELI + key.substring(4, 6)
						+ DAY_DELI + key.substring(6, 8) + "T"
						+ key.substring(8, 10) + ":00:00+0800";
				break;
			case 12:
				// yyyyMMddHHmm
				ret = key.substring(0, 4) + DAY_DELI + key.substring(4, 6)
						+ DAY_DELI + key.substring(6, 8) + "T"
						+ key.substring(8, 10) + HOUR_DELI
						+ key.substring(10, 12) + HOUR_DELI + "00+0800";
				break;
			}

			return ret;
		}// end if

		return null;
	}
}
