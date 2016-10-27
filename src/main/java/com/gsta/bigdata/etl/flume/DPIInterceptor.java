package com.gsta.bigdata.etl.flume;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public class DPIInterceptor implements Interceptor {
	private String delimiter;
	private String headerName;
	private int[] fields;
	private int keyField;
	private static final String NotSeeCharDefineInConf = "001";
	private String outputDelimiter;

	public DPIInterceptor(String delimiter, int[] fields, int keyField,
			String headerName) {
		super();
		this.delimiter = delimiter;
		this.fields = fields;
		this.headerName = headerName;
		this.keyField = keyField;
		
		this.outputDelimiter = StringEscapeUtils.unescapeJava(this.delimiter);
		if (NotSeeCharDefineInConf.equals(this.delimiter)) {
			this.delimiter = "\001";
		}
	}

	@Override
	public void initialize() {
		
	}

	@Override
	public Event intercept(Event event) {
		if (event == null) {
			return null;
		}

		String line = new String(event.getBody());
		if (line == null || "".equals(line)) {
			return null;
		}

		String[] fieldValues = line.split(this.delimiter, -1);
		if (fieldValues == null) {
			return null;
		}

		StringBuffer sb = new StringBuffer();
		boolean flag = false;
		for (int i : this.fields) {
			if (i > 0 && i <= fieldValues.length) {
				sb.append(fieldValues[i - 1]).append(this.outputDelimiter);
				flag = true;
			}else{
				sb.append("").append(this.outputDelimiter);
			}
		}
		if (flag) {
			line = sb.toString().substring(0,sb.length() - this.outputDelimiter.length());
		}
		
		event.setBody(line.getBytes());
		if (this.keyField > 0 && this.keyField <= fieldValues.length) {
			event.getHeaders().put(this.headerName,fieldValues[this.keyField - 1]);
		}

		return event;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> retEvents = new ArrayList<Event>();

		for (Event event : events) {
			Event interceptedEvent = intercept(event);
			if (interceptedEvent != null) {
				retEvents.add(interceptedEvent);
			}
		}

		return retEvents;
	}

	@Override
	public void close() {

	}

	public static class Builder implements Interceptor.Builder {
		private String delimiter;
		private String strFields;
		private String headerName;
		private String strKeyField;
		private int[] fields;
		private int keyField;

		@Override
		public void configure(Context context) {
			this.delimiter = context.getString("delimiter");
			this.strFields = context.getString("fields");
			this.headerName = context.getString("headerName");
			this.strKeyField = context.getString("keyField");

			if (this.strFields != null) {
				String[] strs = this.strFields.split(",", -1);
				this.fields = new int[strs.length];
				for (int i = 0; i < strs.length; i++) {
					this.fields[i] = Integer.parseInt(strs[i]);
				}
			}

			if (this.strKeyField != null) {
				this.keyField = Integer.parseInt(this.strKeyField);
			}
		}

		@Override
		public Interceptor build() {
			return new DPIInterceptor(this.delimiter, this.fields,
					this.keyField, this.headerName);
		}
	}

	public static void main(String[] args) {
		String s = "1|4601104310583|8618125640|8679310927614|CTNET|163.177.81.139|80|100.85.92.123|39915|8.128.0.205|115.169.194.37|115.169.132.149|4601186B4930|460117A5C|46011|6|18|205|20161024095948|20161024095948|0|0|748|0|1|20161023014337|5|5008000000000000000000000|1|8.142.65.45|2152|2152|1286736896|61140717||||||||100.85.92.123|39915|0|163.177.81.139|80|1|0|0|0|0|0|0|0|0|0|0|0|0|0|1|MicroMessenger Client|szextshort.weixin.qq.com/mmtls/32be6ba0|szextshort.weixin.qq.com|szextshort.weixin.qq.com|474|application/octet-stream|0||5|-2|0|0|1477274388418|1477274388418||1477274388418|1477274388418||||3||7235434166285||||||99|";
		Context ctx = new Context();
		ctx.put("delimiter", "\\|");
		ctx.put("fields", "2,3,10,13");
		ctx.put("headerName", "key");
		ctx.put("keyField", "3");
		
		DPIInterceptor.Builder builder = new DPIInterceptor.Builder();
		builder.configure(ctx);
		
		DPIInterceptor i = (DPIInterceptor)builder.build();
		i.initialize();
		Event event = new org.apache.flume.event.SimpleEvent();
		event.setBody(s.getBytes());
		i.intercept(event);
		System.out.println(new String(event.getBody()));
		System.out.println(event.getHeaders());
	}
}
