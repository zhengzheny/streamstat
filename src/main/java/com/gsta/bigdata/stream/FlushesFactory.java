package com.gsta.bigdata.stream;

import com.gsta.bigdata.stream.utils.Constants;

public class FlushesFactory {
	public static IFlush createFlush(String name) {
		if (Constants.FLUSH_CONSOLE.equals(name)) {
			return new ConsoleFlush();
		}else if(Constants.FLUSH_MYSQL.equals(name)){
			return new MySQLFlush();
		}else if(Constants.FLUSH_OPENTSDB.equals(name)){
			return new OpenTSDBFlush();
		}else if(Constants.FLUSH_SIMPLE_REDIS.equals(name)){
			return new SimpleRedisFlush();
		}else if(Constants.FLUSH_MDN_DAY_REDIS.equals(name)){
			return new MDNDayDataRedisFlush();
		}
		
		//default is console flush
		return new ConsoleFlush();
	}
}
