package com.gsta.bigdata.stream.counter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.BloomFilterFactory;
import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.WindowTime;

public class Area5MinCounter extends AbstractCounter {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	// flush time gap,flush counter result
	private long flushTimeGap;
	// time gap for flush
	private long repeatCount = 1;
//	ecgi
	private String[] traincgi = {"460110123285762","460110122855936","460110122881025",
								 "460110122881024","460110122860544","460110122860545"
								};

	private String[] southtraincgi = {"460110220399616","460110219320881","460110219320882",
	                        		  "460110124497968","460110124497969","460110124497970",
	                                  "460110124497713","460110219367984","460110219367986",
	                                  "460110219298866","460110218148400","460110218148401",
							          "460110219243829","460110219298864","460110219298865",
							          "460110124497712","460110124497714","460110219320880"};
	
	private String[] phoneExpocgi = {"460110122818352"
			};
	
	public Area5MinCounter(String name) {
		super(name);
//		5min
		double t = 300 * 1000 * ConfigSingleton.getInstance()
				.getCounterFlushTimeGapRatio(super.name);
		this.flushTimeGap = (long) t;
	 }

	@Override
	public void add(String kafkaKey, Map<String, String> valueData, long timeStamp) {
		if (kafkaKey == null || valueData == null) {
			return;
		}
		
		ArrayList<String> TrainECGIList = new ArrayList<String>(Arrays.asList(traincgi));
		ArrayList<String> SounthTrainECGIList = new ArrayList<String>(Arrays.asList(southtraincgi));
		ArrayList<String> PhoneExpoECGIList = new ArrayList<String>(Arrays.asList(phoneExpocgi));
		String ECGI = valueData.get(Constants.FIELD_ECGI);
		String type = "";
		boolean isExist = true;
		boolean flag = false;
		String selectedFilter= "";
		String area = "";
		WindowTime.WinTime winTime = WindowTime.get5min(timeStamp);
		String ts = winTime.getTimeStamp();
		if(TrainECGIList.contains(ECGI)){
			type = "TrainStation";
			selectedFilter = "5mintrain-mdn-bloomFilter";
			flag = true;
			area = "5mintrainstation";
		}
		else if (SounthTrainECGIList.contains(ECGI)){
			type = "SouthTrainStation";
			selectedFilter = "5minsouthtrain-mdn-bloomFilter";
			flag = true;
			area = "5minsouthtrainstation";
		}
		else if (PhoneExpoECGIList.contains(ECGI)){
			type = "PhoneExpo";
			selectedFilter = "5minphoneexpo-mdn-bloomFilter";
			flag = true;
			area = "5minphoneexpo";
		}
		if (flag){	
				isExist = BloomFilterFactory.getInstance().isExist(
					selectedFilter, timeStamp, valueData);
				if (!isExist) {	
//					key=userstat#0#0#ts,代表count值为userstat，type为火车站或者火车南站
					String key =  type+Constants.KEY_DELIMITER+"userstat" + Constants.KEY_DELIMITER + "0" + Constants.KEY_DELIMITER +"0" + Constants.KEY_DELIMITER+ ts;
					super.addCount(key);
					super.addCountTimeStamp(key);
				}else
					{
				this.repeatCount++;
					}
				if(this.repeatCount % 1000 == 0){
				logger.info("{} has repeat count={}",type+" userstat",this.repeatCount);
				}
								
//				userdata:流量统计
				long inputOctets = 0, outputOctets = 0;
				try {
					inputOctets = Long.parseLong(valueData.get(Constants.FIELD_InputOctets));
					outputOctets = Long.parseLong(valueData.get(Constants.FIELD_OutputOctets));
					} catch (NumberFormatException e) {
					logger.error(e.getMessage());
					return;
					}
				long mdnData = inputOctets + outputOctets;
				String key =  type+Constants.KEY_DELIMITER+"0" + Constants.KEY_DELIMITER +"usetdata" + Constants.KEY_DELIMITER+"0" + Constants.KEY_DELIMITER + ts;
				super.addCount(key, mdnData);
				super.addCountTimeStamp(key);
				BloomFilterFactory.getInstance().add(area,timeStamp, valueData);
		}
		
	}

	@Override
	public long getFlushTimeGap() {
		return this.flushTimeGap;
	}

}
