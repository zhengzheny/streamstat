package com.gsta.bigdata.stream.utils;

import java.util.Calendar;

public class WindowTime {	
	public final static WinTime get1min(long longDate) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(longDate);
		
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH) + 1;
		int day = calendar.get(Calendar.DATE);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);

		StringBuffer ret = new StringBuffer();
		ret.append(addZero(year)).append(addZero(month)).append(addZero(day))
				.append(addZero(hour)).append(addZero(minute));

		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return new WinTime(ret.toString(), calendar.getTimeInMillis());
	}
	
	public final static WinTime get5min(long longDate) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(longDate);
		
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH) + 1;
		int day = calendar.get(Calendar.DATE);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		minute = (minute / 5) * 5;

		StringBuffer ret = new StringBuffer();
		ret.append(addZero(year)).append(addZero(month)).append(addZero(day))
				.append(addZero(hour)).append(addZero(minute));

		calendar.set(Calendar.MINUTE, minute);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return new WinTime(ret.toString(), calendar.getTimeInMillis());
	}

	public final static WinTime get10min(long longDate) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(longDate);
		
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH) + 1;
		int day = calendar.get(Calendar.DATE);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		minute = (minute / 10) * 10;

		StringBuffer ret = new StringBuffer();
		ret.append(addZero(year)).append(addZero(month)).append(addZero(day))
				.append(addZero(hour)).append(addZero(minute));

		calendar.set(Calendar.MINUTE, minute);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return new WinTime(ret.toString(), calendar.getTimeInMillis());
	}

	public final static WinTime get1hour(long longDate) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(longDate);
		
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH) + 1;
		int day = calendar.get(Calendar.DATE);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);

		StringBuffer ret = new StringBuffer();
		ret.append(addZero(year)).append(addZero(month)).append(addZero(day))
				.append(addZero(hour));

		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return new WinTime(ret.toString(), calendar.getTimeInMillis());
	}
	
	public final static WinTime get1day(long longDate) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(longDate);
		
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH) + 1;
		int day = calendar.get(Calendar.DATE);

		StringBuffer ret = new StringBuffer();
		ret.append(addZero(year)).append(addZero(month)).append(addZero(day));
		
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return new WinTime(ret.toString(), calendar.getTimeInMillis());
	}

	private final static String addZero(int i) {
		return String.format("%0" + 2 + "d", i);
	}

	public static class WinTime{
		private String timeStamp;
		private long timeInMillis;
		
		public WinTime(String timeStamp, long timeInMillis) {
			super();
			this.timeStamp = timeStamp;
			this.timeInMillis = timeInMillis;
		}

		public String getTimeStamp() {
			return timeStamp;
		}

		public long getTimeInMillis() {
			return timeInMillis;
		}
		
		public String toString(){
			return "timeStamp="+timeStamp + ",timeInMillis=" + timeInMillis;
		}
	}
	
	public static void main(String[] args) {
		long s = System.currentTimeMillis();
		System.out.println(WindowTime.get1min(s));
		System.out.println(WindowTime.get5min(s));
		System.out.println(WindowTime.get10min(s));
		System.out.println(WindowTime.get1hour(s));
		System.out.println(WindowTime.get1day(s));
	}
}
