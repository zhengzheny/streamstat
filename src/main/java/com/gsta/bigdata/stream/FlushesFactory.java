package com.gsta.bigdata.stream;

import com.gsta.bigdata.stream.utils.Constants;

public class FlushesFactory {
	public static IFlush createFlush(String name) {
		if (Constants.FLUSH_CONSOLE.equals(name)) {
			return new ConsoleFlush();
		}  else if (Constants.FLUSH_SIMPLE_REDIS.equals(name)) {
			return new SimpleRedisFlush();
		} else if (Constants.FLUSH_MDN_DAY_REDIS.equals(name)) {
			return new MDNDayDataRedisFlush();
		} else if (Constants.FLUSH_ELASTICSEARCH.equals(name)) {
			return new ElasticsearchFlush();
		}

		// default is console flush
		return new ConsoleFlush();
	}
}
