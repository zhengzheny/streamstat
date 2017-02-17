package com.gsta.bigdata.stream.utils;

import java.io.InputStream;

/**
 * 
 * @author tianxq
 *
 */
public class JavaResourceUtils {
	public InputStream load(URLocation url) throws Exception {
		if (!url.hasPath()) {
			throw new Exception("Can not find path from url:" + url.toString());
		}
		return load(url.getPath());
	}
	
	public InputStream load(String path)  throws Exception {
	    ClassLoader cl = Thread.currentThread().getContextClassLoader();
		InputStream in = cl.getResourceAsStream(path);
		if (in == null){
			cl = JavaResourceUtils.class.getClassLoader();
			in = cl.getResourceAsStream(path);
			if (in == null){
				@SuppressWarnings("rawtypes")
				Class clazz = getClass();	
				in = clazz.getResourceAsStream(path);
			}
		}
	    return in;
	}
}
