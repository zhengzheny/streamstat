package com.gsta.bigdata.stream.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

public class SysUtils {
	public static final int getProcessID() {
		RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
		
		return Integer.valueOf(runtimeMXBean.getName().split("@")[0]).intValue();
	}
}
