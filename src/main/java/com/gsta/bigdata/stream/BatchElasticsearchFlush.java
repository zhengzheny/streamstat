package com.gsta.bigdata.stream;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;

public class BatchElasticsearchFlush implements IFlush {
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	private final static String DAY_DELI = "-";
	private final static String HOUR_DELI = ":";
	
	private String indexName;
	private BulkProcessor bulkProcessor=null;  
	private TransportClient client;
	
	public BatchElasticsearchFlush() {
		String clusterName = (String) ConfigSingleton.getInstance().getElasticsearchConf().get("cluster.name");
		boolean sniff = (boolean) ConfigSingleton.getInstance().getElasticsearchConf().get("client.transport.sniff");
		
		Settings settings = Settings.builder()
	            .put("cluster.name", clusterName)
	            .put("client.transport.sniff", sniff).build();		
		
		int port = (int) ConfigSingleton.getInstance().getElasticsearchConf().get("port");
		this.client = new PreBuiltTransportClient(settings);
		@SuppressWarnings("unchecked")
		List<String> servers = (List<String>) ConfigSingleton.getInstance().getElasticsearchConf().get("cluster.nodes");
		for (String server : servers) {
			try {
				this.client.addTransportAddress(new InetSocketTransportAddress(
						InetAddress.getByName(server), port));
			} catch (UnknownHostException e) {
				logger.error("init elasticsearch occur error:" + e.toString());
			}
		}// end for
		
		int batchRecord = (int) ConfigSingleton.getInstance().getElasticsearchConf().get("batchRecord");
		int batchSize = (int) ConfigSingleton.getInstance().getElasticsearchConf().get("batchSize");
		int numThread = (int) ConfigSingleton.getInstance().getElasticsearchConf().get("numThread");
		int flushTime = (int) ConfigSingleton.getInstance().getElasticsearchConf().get("flushTime");
		
		bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long id, BulkRequest req) {
				//logger.info("id: " + id + " req: " + req.toString());
			}

			// send fail
			@Override
			public void afterBulk(long id, BulkRequest req,Throwable cause) {
				logger.warn("id: " + id + "  req: " + req.toString() + "  cause: " + cause.getMessage());
			}

			//send success
			@Override
			public void afterBulk(long id, BulkRequest req,BulkResponse rep) {
				logger.info("id: " + id + "  req: " + req + "  rep: " + rep.toString());
			}
		}).setBulkActions(batchRecord)                                 // bulk record
		.setBulkSize(new ByteSizeValue(batchSize, ByteSizeUnit.MB))     // bulk size
		.setConcurrentRequests(numThread)                              //concurrent thread size
		.setFlushInterval(TimeValue.timeValueSeconds(flushTime))       //flush time 
		.build();
		
		this.indexName = (String) ConfigSingleton.getInstance().getElasticsearchConf().get("index.name");
	}

	@Override
	public void flush(String counterName, String key,
			Map<String, String> fieldValues, String timeStamp, long count,
			int processId, String ip) {
		Map<String,Object> map = new HashMap<String,Object>();
		
		if(fieldValues != null && fieldValues.size() > 0)  {
			map.putAll(fieldValues);
		}
		map.put("timeStamp",this.formatTimestamp(timeStamp));
		map.put("count", count);
		map.put("processId", processId);
		map.put("ip", ip);
		map.put("counterName", counterName);
		String tempKey = key + Constants.KEY_DELIMITER + processId + 
				Constants.KEY_DELIMITER + ip;
		
		long t = System.currentTimeMillis();
		String s = String.valueOf(t);
		tempKey += Constants.KEY_DELIMITER + s.substring(s.length()-5);
		
		this.bulkProcessor.add(new IndexRequest(this.indexName, counterName, tempKey).source(map));
	}

	@Override
	public void close() {
		try {
			this.bulkProcessor.awaitClose(3, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private String formatTimestamp(String key) {
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
