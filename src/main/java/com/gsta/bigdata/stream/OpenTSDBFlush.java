package com.gsta.bigdata.stream;

import java.io.IOException;

import org.opentsdb.client.ExpectResponse;
import org.opentsdb.client.HttpClient;
import org.opentsdb.client.HttpClientImpl;
import org.opentsdb.client.builder.MetricBuilder;
import org.opentsdb.client.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.SysUtils;

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
					.setDataPoint(SysUtils.key2timestamp(timeStamp), count)
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
