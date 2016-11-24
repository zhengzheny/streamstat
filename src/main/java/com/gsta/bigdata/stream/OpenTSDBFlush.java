package com.gsta.bigdata.stream;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.opentsdb.client.ExpectResponse;
import org.opentsdb.client.HttpClient;
import org.opentsdb.client.HttpClientImpl;
import org.opentsdb.client.builder.MetricBuilder;
import org.opentsdb.client.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;

public class OpenTSDBFlush implements IFlush {
	private HttpClient client;
	private MetricBuilder builder;
	final Logger logger = LoggerFactory.getLogger(this.getClass());

	public OpenTSDBFlush() {
		String url = ConfigSingleton.getInstance().getOpenTSDBURL();
		if (url != null) {
			this.client = new HttpClientImpl(url);
		} else {
			this.logger.error("open tsdb url is null...");
		}

		this.builder = MetricBuilder.getInstance();
	}
	
	@Override
	public void flush(String counterName, String keyField, String timeStamp,
			long count, int processId) {
		//only flush timestamp key
		if (keyField == null) {
			builder.addMetric(counterName)
					.setDataPoint(this.key2timestamp(timeStamp), count)
					.addTag("processId", String.valueOf(processId));

			try {
				Response response = client.pushMetrics(builder,
						ExpectResponse.SUMMARY);
				logger.info("counterName=" + counterName + ",timeStamp="
						+ timeStamp + ",count=" + count + ",tsd:"
						+ response.toString());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public long key2timestamp(String key){
		if (key != null) {
			try {
				long ret = 0L;
				SimpleDateFormat sdf;
				switch (key.length()) {
				case 8:
					sdf = new SimpleDateFormat("yyyyMMdd");
					ret = sdf.parse(key).getTime();
					break;
				case 10:
					sdf = new SimpleDateFormat("yyyyMMddHH");
					ret = sdf.parse(key).getTime();
					break;
				case 12:
					sdf = new SimpleDateFormat("yyyyMMddHHmm");
					ret = sdf.parse(key).getTime();
					break;
				}

				return ret / 1000L;
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}//end if
		
		return System.currentTimeMillis()/1000;
	}

	@Override
	public void close() {
		if(this.client != null){
			try {
				this.client.shutdown();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
