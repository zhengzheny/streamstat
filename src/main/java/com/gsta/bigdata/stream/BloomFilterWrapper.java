package com.gsta.bigdata.stream;

public class BloomFilterWrapper {
	private BloomFilter<String> bloomFilter;
	private String name;
	
	public BloomFilterWrapper(String name,int expectedSize,double falsePositiveProbability) {
		this.name = name;
		this.bloomFilter = new BloomFilter<String>(falsePositiveProbability, expectedSize);
	}
	
	public void add(String mdn) {
		if (!this.bloomFilter.contains(mdn)) {
			this.bloomFilter.add(mdn);
		}
	}
	
	public boolean isExist(String mdn){
		return this.bloomFilter.contains(mdn);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public void clear(){
		this.bloomFilter.clear();
	}
}
