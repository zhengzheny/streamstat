package com.gsta.bigdata.stream.utils;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class MySQLConnSingleton {
	private static MySQLConnSingleton singleton = new MySQLConnSingleton();
	private static ComboPooledDataSource ds;

	public MySQLConnSingleton() {
		try {
			ds = new ComboPooledDataSource();

			Map<String, Object> confs = ConfigSingleton.getInstance().getMySQLConf();
			if (confs != null) {
				ds.setDriverClass("com.mysql.jdbc.Driver");
				ds.setJdbcUrl((String) confs.get("url"));
				ds.setUser((String) confs.get("user"));
				ds.setPassword((String) confs.get("pwd"));

				ds.setInitialPoolSize((int) confs.get("initialPoolSize"));
				ds.setMaxPoolSize((int) confs.get("maxPoolSize"));
				ds.setMinPoolSize((int) confs.get("minPoolSize"));
				ds.setTestConnectionOnCheckin((boolean) confs.get("testConnectionOnCheckin"));
				ds.setAutomaticTestTable((String) confs.get("automaticTestTable"));
				ds.setIdleConnectionTestPeriod((int) confs.get("idleConnectionTestPeriod"));
				ds.setMaxIdleTime((int) confs.get("maxIdleTime"));
				ds.setCheckoutTimeout((int) confs.get("checkoutTimeout"));
			}
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
	}

	public static MySQLConnSingleton getInstance() {
		return singleton;
	}

	public static Connection getConnection() throws SQLException {
		return ds.getConnection();
	}

	public static void close() {
		ds.close();
	}
}
