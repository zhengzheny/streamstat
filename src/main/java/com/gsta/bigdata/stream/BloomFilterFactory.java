package com.gsta.bigdata.stream;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.ConfigSingleton;
import com.gsta.bigdata.stream.utils.Constants;
import com.gsta.bigdata.stream.utils.WindowTime;

/**
 * 全局共享布隆过滤
 * 第一个版本BloomFilterFactoryOld采用两个时间窗口方式,由于数据源来的时间不及时,如某一个小时的数据,从开始来的时间到结束,有时候有3个小时,
 * 导致统计在线用户数不准,新版本设置固定的布隆过滤器,如果全部满了,把最久不使用的过滤器清除掉,用作新的布隆过滤
 * @author tianxq
 *
 */
public class BloomFilterFactory {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private static BloomFilterFactory singleton = new BloomFilterFactory();
	//第一个key是布隆过滤器名称,如5min-bloomFilter,第二个key是时间窗口
	private Map<String, Map<String,BloomFilterWrapper>> bloomFilters ;
	//第一个key是布隆过滤器名称,如5min-bloomFilter,队列中保存布隆过滤的key,当队列满时,把队列头部过滤器移除，并清空,留着下一个时间窗口用
	private Map<String,Queue<String>> bloomQueue; 
	//每一种过滤器的大小,key是布隆过滤器名称,如5min-bloomFilter
	private Map<String,Integer> filterSize ;
	//每一种过滤器的字段列表
	private Map<String,List<String>> filterFields;
	private Map<String,String> filterTimeGap;
	
	private int expectedSize = 1000;
	private double falsePositiveProbability = 0.1;
	//是否开启布隆过滤开关
	private boolean filter = false;
	private final static int DEFAULT_BLOOM_FILTER_COUNT = 2;

	private BloomFilterFactory() {
		this.filterSize = new HashMap<String,Integer>();
		this.bloomFilters = new HashMap<String, Map<String,BloomFilterWrapper>>();
		this.bloomQueue = new HashMap<String,Queue<String>>();	
		this.filterFields = new HashMap<String,List<String>>();
		this.filterTimeGap = new HashMap<String,String>();
	}

	@SuppressWarnings("unchecked")
	public void init() {
		Map<String, Object> conf = ConfigSingleton.getInstance().getBloomFilter();
		if (conf == null) {
			logger.error("invalid bloom filter config...");
			return;
		}
		
		this.falsePositiveProbability = (double) conf.getOrDefault("falsePositiveProbability", 0.001);
		this.expectedSize = (int) conf.getOrDefault("expectedSize", 1000000);
		
		this.filter = (boolean) conf.getOrDefault("filter",false);
		List<String> names = (List<String>) conf.get("name");
		if (this.filter && names != null) {
			for (String name : names) {
				this.bloomFilters.put(name, new ConcurrentHashMap<String, BloomFilterWrapper>());
				
				ConcurrentLinkedQueue<String> q = new ConcurrentLinkedQueue<String>();
				this.bloomQueue.put(name, q);
				
				Map<String,Object> filterConf = (Map<String,Object>)conf.get(name);
				if(filterConf == null){
					throw new RuntimeException(name + " filter has no configure...");
				}
				String fields = (String)filterConf.get("fields");
				if(fields == null){
					throw new RuntimeException(name + " filter has no fields configure...");
				}				
				List<String> arrFields = Arrays.asList(fields.split(","));
				this.filterFields.put(name, arrFields);
				int size = (Integer)filterConf.getOrDefault("size", DEFAULT_BLOOM_FILTER_COUNT);
				this.filterSize.put(name, size);
				logger.info("bloom filter,name={},size={},fields={}",name,size,arrFields);
				
				String timeGap = (String)filterConf.getOrDefault("timeGap", Constants.TIME_GAP_5_MIN);
				this.filterTimeGap.put(name, timeGap);
			}
		}
	}
	
	/**
	 * 得到过滤器的去重关键字,如果有多个字段,就有多个字段组成
	 * @param filterName
	 * @param data
	 * @return
	 */
	private  String getFilterKey(String filterName,Map<String, String> data){
		String ret = "";
		
		if(data != null){
			for(String field:this.filterFields.get(filterName)){
				ret += data.get(field);
			}
		}
		
		return ret;
	}

	/**
	 * 向布隆过滤器增加数据
	 * @param timeStamp - 时间戳,根据时间戳找到对应的布隆过滤其
	 * @param mdn - 电话号码
	 */
	public void add(long timeStamp,Map<String, String> data){
		if (!this.filter) return;

		// 一个个过滤器处理,5分钟/1小时/1天
		for (Map.Entry<String, Map<String, BloomFilterWrapper>> mapEntry : this.bloomFilters.entrySet()) {
			String filterName = mapEntry.getKey();
			Map<String, BloomFilterWrapper> bloomFilter = mapEntry.getValue();
			
			WindowTime.WinTime winTime = this.getWindowKey(filterName,timeStamp);
			String timekey = winTime.getTimeStamp();
			
			String filterKey = this.getFilterKey(filterName, data);
			
			Queue<String> queue = this.bloomQueue.get(filterName);
			//第一次还没有过滤器,创建初始化
			if (queue.isEmpty()) {				
				this.createBloomFilter(filterName, timekey, filterKey);
			} else {
				//如果队列包含时间key,直接插入
				if(queue.contains(timekey)){
					BloomFilterWrapper bloomFilterWrapper = bloomFilter.get(timekey);
					if(bloomFilterWrapper != null) {
						bloomFilterWrapper.add(filterKey);
					}else{
						//如果在队列中有,但是过滤器没有,即两边不一致,告警,丢掉此条数据
						logger.error("get null bloomFilterWrapper,key={}",timekey);
					}
				}else{
					//如果队列满,得到最久不使用,并重置过滤器
					if(queue.size() >= this.filterSize.getOrDefault(filterName,DEFAULT_BLOOM_FILTER_COUNT).intValue()){
						String oldkey = queue.poll();
						BloomFilterWrapper bloomFilterWrapper = bloomFilter.get(oldkey);
						if(bloomFilterWrapper != null){
							bloomFilterWrapper.clear();
							
							bloomFilterWrapper.add(filterKey);
							
							bloomFilter.remove(oldkey);
							bloomFilter.put(timekey, bloomFilterWrapper);
							queue.offer(timekey);
							logger.info("{} switch bloom filter,old key={},new key={}",filterName,oldkey,timekey);
							this.printCacheSize(filterName);
						}else{
							//如果队列和内存不一致,只有重建一个
							logger.warn("get null old bloomFilterWrapper,key={},create new filter",oldkey);
							this.createBloomFilter(filterName, timekey, filterKey);
						}
					}else{
						//队列不满,直接新加,并插入
						this.createBloomFilter(filterName, timekey, filterKey);
					}
				}
			}
		}//end for map
	}

	private void createBloomFilter(String filterName, String timeKey, String filterKey) {
		BloomFilterWrapper bloomFilterWrapper = new BloomFilterWrapper(timeKey,
				this.expectedSize, this.falsePositiveProbability);
		bloomFilterWrapper.add(filterKey);

		//增加过滤器和队列
		this.bloomFilters.get(filterName).put(timeKey, bloomFilterWrapper);
		this.bloomQueue.get(filterName).offer(timeKey);
		logger.info("{} create bloom filter {}",filterName,timeKey);
		this.printCacheSize(filterName);
	}
	
	private void printCacheSize(String filterName){
		int bloomfilterSize = this.bloomFilters.get(filterName).size();
		int queueSize = this.bloomQueue.get(filterName).size();
		logger.info("{} has {} bloom filters,queue size={}",filterName,bloomfilterSize,queueSize);
	}
	
	/**
	 * 判断号码是否在过滤器中
	 * @param filterName
	 * @param timeStamp
	 * @param mdn
	 * @return
	 */
	public boolean isExist(String filterName,long timeStamp,Map<String, String> data){
		Map<String, BloomFilterWrapper> bloomFilter = this.bloomFilters.get(filterName);
		if(bloomFilter != null){
			WindowTime.WinTime winTime = this.getWindowKey(filterName,timeStamp);
			BloomFilterWrapper bloomFilterWrapper = bloomFilter.get(winTime.getTimeStamp());
			if(bloomFilterWrapper != null){
				String filterKey = this.getFilterKey(filterName, data);
				return bloomFilterWrapper.isExist(filterKey);
			}
		}
		
		return false;
	}

	private WindowTime.WinTime getWindowKey(String filterName,long timeStamp){
		String timeGap = this.filterTimeGap.getOrDefault(filterName, Constants.TIME_GAP_5_MIN);
		if (Constants.TIME_GAP_5_MIN.equals(timeGap)) {
			return WindowTime.get5min(timeStamp);
		} else if (Constants.TIME_GAP_1_HOUR.equals(timeGap)) {
			return WindowTime.get1hour(timeStamp);
		}else if (Constants.TIME_GAP_1_DAY.equals(timeGap)) {
			return WindowTime.get1day(timeStamp);
		}
		
		return null;
	}

	public static BloomFilterFactory getInstance() {
		return singleton;
	}

	public static void main(String[] args){
		
	}
}
