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
	
	public static boolean isIPV4(String strIP) {
		if (strIP == null || "".equals(strIP)) {
			return false;
		}

		int count = 0;
		final String letters = "1234567890.";
		int length = strIP.length();
		for (int i = 0; i < length; i++) {
			char c = strIP.charAt(i);
			if (letters.indexOf(c) == -1) {
				return false;
			}
			if (strIP.charAt(i) == '.')
				count++;
		}

		if (count != 3) {
			return false;
		}

		String[] parts = new String[4];
		int first = strIP.indexOf(".");
		int last = strIP.lastIndexOf(".");
		parts[0] = strIP.substring(0, first);
		String subip = strIP.substring(0, last);
		int sublength = subip.length();
		int second = subip.lastIndexOf(".");
		parts[1] = subip.substring(first + 1, second);
		parts[2] = subip.substring(second + 1, sublength);
		parts[3] = strIP.substring(last + 1, length);

		for (int i = 0; i < parts.length; i++) {
			if (parts[i] == null || parts[i].equals("")) {
				return false;
			}

			int temp = Integer.parseInt(parts[i]);
			if (temp < 0 || temp > 255) {
				return false;
			}
		}

		return true;
	}
	
	public static String getThirdPartDomain(String domain){
		if(domain == null) return null;
		
		String host = domain;
		int pos = domain.lastIndexOf(":");
		if(pos > 0){
			host = domain.substring(0, pos);
		}
		
		//如果是ipv4,直接返回
		if(isIPV4(host))  return domain;
		
		//否则取第三级域名
		String ret = domain;
		String[] subDomain = domain.split("\\.");
		int len = subDomain.length;
		if(subDomain != null && len > 3){
			StringBuffer sb = new StringBuffer();
			sb.append(subDomain[len-3]).append(".").
			append(subDomain[len-2]).append(".").append(subDomain[len-1]);
			
			ret = sb.toString();
		}
		
		return ret;
	}
}
