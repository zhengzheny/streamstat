package com.gsta.bigdata.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.gsta.bigdata.stream.utils.WindowTime;

@Deprecated
public class WindowBloomFilter {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	// keep two window's data
	private BlockBloomFilter kbf1;
	private BlockBloomFilter kbf2;
	private long ignoreCount = 0L;
	// time gap for flush
	private long timeGap;

	public WindowBloomFilter(long timeGap, int expectedSize,
			double falsePositiveProbability) {
		this.kbf1 = new BlockBloomFilter("first bloom filter", expectedSize,falsePositiveProbability);		
		this.kbf2 = new BlockBloomFilter("second bloom filter", expectedSize,falsePositiveProbability);
		this.timeGap = timeGap;
	}

	/**
	 * 
	 * @param timeStamp - the data timeStamp from kafka
	 * @param winTime - window time according to timeStamp
	 * @param mdn - user mobile number
	 * @param counter
	 * @param key - the key of counter map
	 */
	public void add(long timeStamp, WindowTime.WinTime winTime,String mdn) {
		long delta1 = timeStamp - this.kbf1.getTimeStamp();
		long delta2 = timeStamp - this.kbf2.getTimeStamp();
		if (this.kbf1.getCount() == -1) {
			//首先考虑第一块分区,没有初始化情况
			this.kbf1.switchKey(winTime);
			this.kbf1.add(mdn);
			logger.info(winTime + " use " + this.kbf1.getName());
		} else if (delta1 >= 0 && delta1 <= this.timeGap) {
			//时间在第一块分区时间窗口内
			this.kbf1.add(mdn);
			logger.debug(mdn + " add " + this.kbf1.getName());
		} else if (this.kbf2.getCount() == -1) {
			//第一块分区用完后,考虑第二块分区没初始化情况
			this.kbf2.switchKey(winTime);
			this.kbf2.add(mdn);
			logger.info(winTime + " use " + this.kbf2.getName());
		} else if (delta2 >= 0 && delta2 <= this.timeGap) {
			//时间在第二块分区时间窗口内
			this.kbf2.add(mdn);
			logger.debug(mdn + " add " + this.kbf2.getName());
		} else if (delta1 < 0 && delta2 < 0
				&& this.kbf1.getCount() != -1 && this.kbf2.getCount() != -1) {
			//如果时间在两个时间窗口前,忽略它
			logger.debug(mdn + " before kbf1 and kbf2,so ignore ");
			this.ignoreCount++;
			if (this.ignoreCount % 1000 == 0)
				logger.info("ignore " + this.ignoreCount + " message...");
		} else {
			//时间在两块分区时间窗口后,把最久不用的分区清空置换出来,分配给新的分区
			BlockBloomFilter kbf = this.getOldestKBF();
			kbf.switchKey(winTime);
			kbf.add(mdn);
			logger.info(winTime + " use " + kbf.getName());
		}
	}

	private BlockBloomFilter getOldestKBF() {
		return this.kbf1.getTimeStamp() > this.kbf2.getTimeStamp() ? 
				this.kbf2 : this.kbf1;
	}
	
	public boolean isExist(long timeStamp,String mdn){
		long delta1 = timeStamp - this.kbf1.getTimeStamp();
		long delta2 = timeStamp - this.kbf2.getTimeStamp();
		if (this.kbf1.getCount() == -1) {
			return this.kbf1.isExist(mdn);
		} else if (delta1 >= 0 && delta1 <= this.timeGap) {
			return this.kbf1.isExist(mdn);
		} else if (this.kbf2.getCount() == -1) {
			return this.kbf2.isExist(mdn);
		} else if (delta2 >= 0 && delta2 <= this.timeGap) {
			return this.kbf2.isExist(mdn);
		} else if (delta1 < 0 && delta2 < 0
				&& this.kbf1.getCount() != -1 && this.kbf2.getCount() != -1) {
			return false;
		} else {
			return this.getOldestKBF().isExist(mdn);
		}
	}

	class BlockBloomFilter {
		// bloom filter to de-repeat user MDN
		private BloomFilter<String> bloomFilter;
		// switch key,increment it
		private int count = -1;
		// the long timeMillis of key,default is current time
		private long timeStamp;
		private String name;

		public BlockBloomFilter(String name, int expectedSize,double falsePositiveProbability) {
			this.name = name;
			this.bloomFilter = new BloomFilter<String>(falsePositiveProbability, expectedSize);
			this.timeStamp = System.currentTimeMillis();
		}

		public void add(String mdn) {
			if (!this.bloomFilter.contains(mdn)) {
				this.bloomFilter.add(mdn);
			}
		}
		
		public boolean isExist(String mdn){
			return this.bloomFilter.contains(mdn);
		}

		public synchronized void switchKey(WindowTime.WinTime winTime) {
			Preconditions.checkNotNull(winTime, "win time obj is null");
			this.timeStamp = winTime.getTimeInMillis();
			this.bloomFilter.clear();
			this.count++;
		}

		public long getTimeStamp() {
			return timeStamp;
		}

		public int getCount() {
			return count;
		}

		public String getName() {
			return name;
		}
	}
}
