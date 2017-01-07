package com.gsta.bigdata.stream.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class SysUtils {
	public static final int getProcessID() {
		RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();

		return Integer.valueOf(runtimeMXBean.getName().split("@")[0]).intValue();
	}
	
	public static final String getLastIp(){
		try {
			InetAddress addr = InetAddress.getLocalHost();
			String ip = addr.getHostAddress().toString();
			ip = ip.substring(ip.lastIndexOf(".") + 1, ip.length());
			return ip;
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		return "-1";
	}

	public static final long key2timestamp(String key) {
		if (key != null) {
			try {
				long ret = 0L;
				SimpleDateFormat sdf;
				switch (key.length()) {
				case 8:
					sdf = new SimpleDateFormat("yyyyMMdd");
					ret = sdf.parse(key).getTime();
					break;
				case 10:
					sdf = new SimpleDateFormat("yyyyMMddHH");
					ret = sdf.parse(key).getTime();
					break;
				case 12:
					sdf = new SimpleDateFormat("yyyyMMddHHmm");
					ret = sdf.parse(key).getTime();
					break;
				}

				return ret / 1000L;
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}// end if

		return System.currentTimeMillis() / 1000;
	}
}
