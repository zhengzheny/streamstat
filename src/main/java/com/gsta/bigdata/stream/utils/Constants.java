package com.gsta.bigdata.stream.utils;

public class Constants {
	@Deprecated
	public final static String BLOOM_FILTER_5MIN = "5min-bloomFilter";
	@Deprecated
	public final static String BLOOM_FILTER_1HOUR = "1hour-bloomFilter";
	@Deprecated
	public final static String BLOOM_FILTER_1DAY = "1day-bloomFilter";
	
	public final static String FIELD_TIMESTAMP = "TimeStamp";
	public final static String FIELD_MSISDN = "MSISDN";
	public final static String FIELD_ECGI = "ECGI";
	public final static String FIELD_SGWIP = "SGWIP";
	public final static String FIELD_InputOctets = "InputOctets";
	public final static String FIELD_OutputOctets = "OutputOctets";
	public final static String FIELD_Domain = "Domain";
	public final static String FIELD_Age = "Age";
	public final static String FIELD_Sex = "Sex";
	
	public final static String FLUSH_CONSOLE = "console";
	public final static String FLUSH_SIMPLE_REDIS = "simpleRedis";
	public final static String FLUSH_MDN_DAY_REDIS = "mdnDayRedis";
	public final static String FLUSH_ELASTICSEARCH = "elasticsearch";
	public final static String FLUSH_Mysql= "Mysql";
	
	public final static String KEY_DELIMITER = "#";
	public final static String REQUEST_KEY_DELIMITER = "#";
	
	public final static String TIME_GAP_5_MIN = "5min";
	public final static String TIME_GAP_1_HOUR = "1hour";
	public final static String TIME_GAP_1_DAY = "1day";
	
	public final static String OUTPUT_FIELD_TIMESTAMP = "timeStamp";
	public final static String OUTPUT_FIELD_COUNT = "count";
	public final static String OUTPUT_FIELD_PROCESSID = "processId";
	public final static String OUTPUT_FIELD_IP = "ip";
	public final static String OUTPUT_FIELD_COUNTER_NAME = "counterName";
	public final static String OUTPUT_FIELD_KEY = "key";
	public final static String OUTPUT_STREAM_NO = "streamCnt";
}
