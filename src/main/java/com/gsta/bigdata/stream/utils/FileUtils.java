package com.gsta.bigdata.stream.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * 
 * @author tianxq
 *
 */
public class FileUtils {
	@SuppressWarnings("resource")
	/**
	 * get input stream according to file name
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	public static InputStream getInputFile(String fileName) throws Exception {
		InputStream inputStream = null;
		try {
			// get configure file from local
			inputStream = new FileInputStream(new File(fileName));
		} catch (FileNotFoundException e) {
			// if found no local file,get configure file from classpath
			if (!fileName.startsWith("java://")) {
				fileName = "java://" + fileName;
			}
			JavaResourceUtils jrl = new JavaResourceUtils();
			URLocation url = new URLocation(fileName);

			inputStream = jrl.load(url);
		}

		return inputStream;
	}
	
	public static String streamToString(InputStream in) throws IOException {
		StringBuffer out = new StringBuffer();
		byte[] b = new byte[4096];
		for (int n; (n = in.read(b)) != -1;) {
			out.append(new String(b, 0, n));
		}
		return out.toString();
	}
}
