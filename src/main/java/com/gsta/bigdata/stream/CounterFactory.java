package com.gsta.bigdata.stream;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.gsta.bigdata.stream.utils.ConfigSingleton;

public class CounterFactory {
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static AbstractCounter createCounter(String name) {
		String type = ConfigSingleton.getInstance().getCounterType(name);
		if (type != null) {
			try {
				Class clazz = Class.forName(type);
				Constructor constructor = clazz.getConstructor(String.class);
				return (AbstractCounter) constructor.newInstance(name);
			} catch (ClassNotFoundException | NoSuchMethodException
					| SecurityException | InstantiationException
					| IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}
	
}
