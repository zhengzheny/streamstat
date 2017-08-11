package com.gsta.bigdata.stream;

import com.gsta.bigdata.stream.flush.ConsoleFlush;
import com.gsta.bigdata.stream.flush.ElasticsearchFlush;
import com.gsta.bigdata.stream.flush.IFlush;
import com.gsta.bigdata.stream.flush.MDNDayDataRedisFlush;
import com.gsta.bigdata.stream.flush.MysqlFlush;
import com.gsta.bigdata.stream.flush.SimpleRedisFlush;
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
		}else if (Constants.FLUSH_Mysql.equals(name)) {
			return new MysqlFlush();
		}
		// default is console flush
		return new ConsoleFlush();
	}
}
