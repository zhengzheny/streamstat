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

public class ESWriterThread implements Runnable{
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	private String indexName;
	private BulkProcessor bulkProcessor=null;  
	private TransportClient client;
	
	public ESWriterThread() {
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
		//int flushTime = (int) ConfigSingleton.getInstance().getElasticsearchConf().get("flushTime");
		
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
		//.setFlushInterval(TimeValue.timeValueSeconds(flushTime))       //flush time 
		.build();
		
		this.indexName = (String) ConfigSingleton.getInstance().getElasticsearchConf().get("index.name");
	}
	

	@Override
	public void run() {
		while (true) {
			Map<String, HashMap<String, Object>> requests = ESCacheSingleton.getSingleton().getRequests();
			int counterSize = requests.size();
			int flushCount = 0;
			
			for (Map.Entry<String, HashMap<String, Object>> mapEntry : requests.entrySet()) {
				String reqKey = mapEntry.getKey();
				HashMap<String, Object> map = mapEntry.getValue();
				
				String[] fields = reqKey.split(Constants.REQUEST_KEY_DELIMITER,-1);
				if(fields.length < 2){
					logger.error("request key=" + reqKey + " is inavlid key...");
				}else{
					String counterName = fields[0];
					String esKey = fields[1];
					this.bulkProcessor.add(new IndexRequest(this.indexName, counterName, esKey).source(map));
				}
				
				requests.remove(reqKey);
				flushCount++;
			}
			
			logger.info("elasticsearch requests size=" + counterSize + ",writeCount=" + flushCount);
			try {
				Thread.sleep(20 * 1000L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void close() {
		try {
			this.bulkProcessor.awaitClose(3, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		if (args.length < 5) {
			System.out.println("usage:ESWriterThread batchRecord batchSize(M) numThread flushTime(second) totalCount");
			System.exit(-1);
		}

		Logger logger = LoggerFactory.getLogger(ElasticsearchFlush.class);
		Settings settings = Settings.builder().put("cluster.name", "gd")
				.put("client.transport.sniff", "true").build();

		TransportClient client = new PreBuiltTransportClient(settings);
		try {
			client.addTransportAddress(new InetSocketTransportAddress(
					InetAddress.getByName("132.122.70.131"), 9300));
			client.addTransportAddress(new InetSocketTransportAddress(
					InetAddress.getByName("132.122.70.132"), 9300));
			client.addTransportAddress(new InetSocketTransportAddress(
					InetAddress.getByName("132.122.70.133"), 9300));
			client.addTransportAddress(new InetSocketTransportAddress(
					InetAddress.getByName("132.122.70.134"), 9300));
			client.addTransportAddress(new InetSocketTransportAddress(
					InetAddress.getByName("132.122.70.135"), 9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		int batchRecord = Integer.parseInt(args[0]);
		long batchSize = Long.parseLong(args[1]);
		int numThread = Integer.parseInt(args[2]);
		long flushTime = Long.parseLong(args[3]);

		BulkProcessor bulkProcessor = BulkProcessor
				.builder(client, new BulkProcessor.Listener() {
					@Override
					public void beforeBulk(long id, BulkRequest req) {
						// logger.info("id: " + id + " req: " + req.toString());
					}

					// send fail
					@Override
					public void afterBulk(long id, BulkRequest req,
							Throwable cause) {
						logger.info("id: " + id + "  req: "+ req.toString() + "  cause: "+ cause.getMessage());
					}

					// send success
					@Override
					public void afterBulk(long id, BulkRequest req,
							BulkResponse rep) {
						logger.info("id: " + id + "  req: " + req+ "  rep: " + rep.toString());
					}
				}).setBulkActions(batchRecord) // bulk record
				.setBulkSize(new ByteSizeValue(batchSize, ByteSizeUnit.MB)) // bulk size
				.setConcurrentRequests(numThread) // concurrent thread size
				.setFlushInterval(TimeValue.timeValueSeconds(flushTime)) // flush time
				.build();

		logger.info("begin test...");
		long t1 = System.currentTimeMillis();
		long totalCount = Long.parseLong(args[4]);
		for (int i = 0; i < totalCount; i++) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("timeStamp", System.currentTimeMillis());
			map.put("count1", i);
			map.put("count2", i);
			map.put("count3", i);
			map.put("count4", i);
			bulkProcessor.add(new IndexRequest("tianxqtest", "test", String.valueOf(i)).source(map));
		}
		long t2 = System.currentTimeMillis();
		long t = (t2-t1)/1000;
		logger.info("write " + totalCount +" record,cost " + t +" second...");
		
		bulkProcessor.close();
	}
}
