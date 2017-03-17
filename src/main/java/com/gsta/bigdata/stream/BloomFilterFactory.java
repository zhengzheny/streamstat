package com.gsta.bigdata.stream;

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
	
	private int expectedSize = 1000;
	private double falsePositiveProbability = 0.1;
	//是否开启布隆过滤开关
	private boolean filter = false;
	private List<String> names;
	private final static int DEFAULT_BLOOM_FILTER_COUNT = 2;

	@SuppressWarnings("unchecked")
	private BloomFilterFactory() {
		this.filterSize = new HashMap<String,Integer>();
		
		Map<String, Object> conf = ConfigSingleton.getInstance().getBloomFilter();
		if (conf != null) {
			this.falsePositiveProbability = (double) conf.getOrDefault(
					"falsePositiveProbability", 0.1);
			this.expectedSize = (int) conf.getOrDefault("expectedSize", 1000);
			this.filter = (boolean)conf.get("filter");
			this.names = (List<String>) conf.get("name");
			
			Map<String,Integer> sizes = (Map<String,Integer>)conf.get("size");
			if(sizes != null){
				this.filterSize.putAll(sizes);
			}
		} else {
			logger.error("invalid bloom filter config...");
		}
		
		this.bloomFilters = new HashMap<String, Map<String,BloomFilterWrapper>>();
		this.bloomQueue = new HashMap<String,Queue<String>>();	
	}

	public void init() {
		if (this.filter && this.names != null) {
			for (String name : names) {
				this.bloomFilters.put(name, new ConcurrentHashMap<String, BloomFilterWrapper>());
				
				ConcurrentLinkedQueue<String> q = new ConcurrentLinkedQueue<String>();
				this.bloomQueue.put(name, q);
				
				//不配置,默认有2个布隆过滤,主要是考虑切换时候用
				int size = this.filterSize.getOrDefault(name,DEFAULT_BLOOM_FILTER_COUNT);
				logger.info("bloom filter,name={},size={}",name,size);
			}
		}
	}

	/**
	 * 向布隆过滤器增加数据
	 * @param timeStamp - 时间戳,根据时间戳找到对应的布隆过滤其
	 * @param mdn - 电话号码
	 */
	public void add(long timeStamp,String mdn){
		if (!this.filter) return;

		// 一个个过滤器处理,5分钟/1小时/1天
		for (Map.Entry<String, Map<String, BloomFilterWrapper>> mapEntry : this.bloomFilters.entrySet()) {
			String filterName = mapEntry.getKey();
			Map<String, BloomFilterWrapper> bloomFilter = mapEntry.getValue();
			
			WindowTime.WinTime winTime = this.getWindowKey(filterName,timeStamp);
			String timekey = winTime.getTimeStamp();
			
			Queue<String> queue = this.bloomQueue.get(filterName);
			//第一次还没有过滤器,创建初始化
			if (queue.isEmpty()) {
				this.createBloomFilter(filterName, timekey, mdn);
			} else {
				//如果队列包含时间key,直接插入
				if(queue.contains(timekey)){
					BloomFilterWrapper bloomFilterWrapper = bloomFilter.get(timekey);
					if(bloomFilterWrapper != null) {
						bloomFilterWrapper.add(mdn);
					}else{
						//如果在队列中有,但是过滤器没有,即两边不一致,告警,丢掉此条数据
						logger.error("get null bloomFilterWrapper,key={}",timekey);
					}
				}else{
					//如果队列满,得到最久不使用,并重置过滤器
					if(queue.size() > this.filterSize.getOrDefault(filterName,DEFAULT_BLOOM_FILTER_COUNT).intValue()){
						String oldkey = queue.poll();
						BloomFilterWrapper bloomFilterWrapper = bloomFilter.get(oldkey);
						if(bloomFilterWrapper != null){
							bloomFilterWrapper.clear();
							bloomFilterWrapper.add(mdn);
							
							bloomFilter.remove(oldkey);
							bloomFilter.put(timekey, bloomFilterWrapper);
							queue.offer(timekey);
							logger.info("switch bloom filter,old key={},new key={}",oldkey,timekey);
						}else{
							//如果队列和内存不一致,只有重建一个
							logger.warn("get null old bloomFilterWrapper,key={},create new filter",oldkey);
							this.createBloomFilter(filterName, timekey, mdn);
						}
					}else{
						//队列不满,直接新加,并插入
						this.createBloomFilter(filterName, timekey, mdn);
					}
				}
			}
		}//end for map
	}

	private void createBloomFilter(String filterName, String timeKey, String mdn) {
		BloomFilterWrapper bloomFilterWrapper = new BloomFilterWrapper(timeKey,
				this.expectedSize, this.falsePositiveProbability);
		bloomFilterWrapper.add(mdn);

		//增加过滤器和队列
		this.bloomFilters.get(filterName).put(timeKey, bloomFilterWrapper);
		this.bloomQueue.get(filterName).offer(timeKey);
		logger.info("create bloom filter {}",timeKey);
	}
	
	/**
	 * 判断号码是否在过滤器中
	 * @param filterName
	 * @param timeStamp
	 * @param mdn
	 * @return
	 */
	public boolean isExist(String filterName,long timeStamp,String mdn){
		Map<String, BloomFilterWrapper> bloomFilter = this.bloomFilters.get(filterName);
		if(bloomFilter != null){
			WindowTime.WinTime winTime = this.getWindowKey(filterName,timeStamp);
			BloomFilterWrapper bloomFilterWrapper = bloomFilter.get(winTime.getTimeStamp());
			if(bloomFilterWrapper != null){
				return bloomFilterWrapper.isExist(mdn);
			}
		}
		
		return false;
	}

	private WindowTime.WinTime getWindowKey(String filterName,long timeStamp){
		if (Constants.BLOOM_FILTER_5MIN.equals(filterName)) {
			return WindowTime.get5min(timeStamp);
		} else if (Constants.BLOOM_FILTER_1HOUR.equals(filterName)) {
			return WindowTime.get1hour(timeStamp);
		}else if (Constants.BLOOM_FILTER_1DAY.equals(filterName)) {
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
