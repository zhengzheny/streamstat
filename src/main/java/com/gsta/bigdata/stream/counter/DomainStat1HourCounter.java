package com.gsta.bigdata.stream.counter;

import java.util.Map;

import com.gsta.bigdata.stream.utils.SysUtils;

public class DomainStat1HourCounter extends AbstractCounter {

	public DomainStat1HourCounter(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void add(String kafkaKey, Map<String, String> valueData, String mdn,
			long timeStamp) {
		// TODO Auto-generated method stub

	}

	@Override
	public long getFlushTimeGap() {
		// TODO Auto-generated method stub
		return 0;
	}

	public static void main(String[] args){
		String domain="szextshort.weixin.qq.com";
		//domain="api.weibo.cn";
		//domain = "qq.com";
		//domain="182.254.116.117:80";
		//domain="182.254.116.117";
		domain = "amdc.m.taobao.com:8090";
		
		System.out.println(SysUtils.getThirdPartDomain(domain));
	}
}
