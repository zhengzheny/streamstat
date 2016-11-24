package com.gsta.bigdata.stream;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.stream.utils.MySQLConnSingleton;

public class MySQLFlush implements IFlush {
	private PreparedStatement ps;
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	private String sql;

	@SuppressWarnings("static-access")
	public MySQLFlush() {
		sql = "insert into 4gDPI(counterName,statkey,statts,processid,statvalue) values (?,?,?,?,?)";
		try {
			ps = MySQLConnSingleton.getInstance().getConnection().prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void flush(String counterName, String keyField, String timeStamp,
			long count, int processId) {
		try {
			logger.info("counterName=" + counterName +
					",keyField=" + keyField +
					",timeStamp=" + timeStamp + 
					",count=" + count );
			ps.setString(1, counterName);
			ps.setString(2, keyField);
			ps.setString(3, timeStamp);
			ps.setInt(4, processId);
			ps.setLong(5,count);
			
			ps.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		if(this.ps != null){
			try {
				this.ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
