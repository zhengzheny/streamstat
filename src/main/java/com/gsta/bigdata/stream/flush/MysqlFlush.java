package com.gsta.bigdata.stream.flush;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.gsta.bigdata.stream.utils.ConfigSingleton.getInstance;
public class MysqlFlush	implements IFlush {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private static String pass = (String)getInstance().getMysqlProperties().get("pass");
	private static String user = (String)getInstance().getMysqlProperties().get("user");
    private static String URL = (String)getInstance().getMysqlProperties().get("URL");
    private static long LastTran1hour= 0;
    private static long LastTran5min = 0;
    private static int size5min = (int)getInstance().getMysqlProperties().get("size5min");
    private static int size1hour = (int)getInstance().getMysqlProperties().get("size1hour");
	protected Connection conn;
	public static ConcurrentHashMap<String,ArrayList<String>> map = new ConcurrentHashMap<String,ArrayList<String>>();
	public static ConcurrentHashMap<String,ArrayList<String>> mapDomain = new ConcurrentHashMap<String,ArrayList<String>>();
	ArrayList<String> list5min = new ArrayList<String>();
	ArrayList<String> list1hour = new ArrayList<String>();
	public MysqlFlush() {

	}
	@Override
	public void flush(String counterName,String key, Map<String, String> fieldValues, String timeStamp,long count) {
//		logger.info("start flush mysql,"+counterName+key);
		String area = fieldValues.get("area");
		if (!area.equals("5minTrain")&&!area.equals("1hourTrain")){
			if(counterName.equals("area-domain-1hour")){
				if (!fieldValues.get("domain").equals("未知协议")){
				list1hour = new ArrayList<String>();
				list1hour.add(counterName);		
				list1hour.add(area);   
				list1hour.add(""); 
		        list1hour.add(timeStamp);
		        list1hour.add(String.valueOf(count));
		        list1hour.add(""); 
		        list1hour.add(fieldValues.get("domain"));
		        mapDomain.put(key,list1hour);
				}
			}else {
//				用户数统计，只发送userstat类型
				String type = fieldValues.get("type");
				if (!type.equals("userdata")){
				list5min = new ArrayList<String>();
				list5min.add(counterName);		
				list5min.add(area); 
				list5min.add(type); 
		        list5min.add(timeStamp);
		        list5min.add(String.valueOf(count));
		        String domain = "";
		        list5min.add(""); 
		        list5min.add(domain);
		        map.put(key,list5min);
				}
			}
        long fqu5min = (System.currentTimeMillis()-LastTran5min)/1000;
//        控制每次发送数据间隔在1s-5min之间
        if(fqu5min>(5*60)){
        	if(size5min>1){
        	size5min--;
        	}
        	logger.info("mysql desc size5min:"+size5min);
        }
//        开始刷5min的
        if (map.size()>=size5min){
        	if (fqu5min<1){
            	size5min++;
            	logger.info("mysql add size5min:"+size5min);
            }
		try {
			Connection conn = DriverManager.getConnection(URL, user, pass);  
	        conn.setAutoCommit(false);  
	        //向mysql中插入数据  
	        String sql = "insert into AreaStat(counterName,area,type,time_stamp,count,stat,domain)values(?,?,?,?,?,?,?)";  
	        PreparedStatement ps = conn.prepareStatement(sql);//要执行sql语句的对象  
//	        	对应？
	       int flushSize = map.size();
	       for (Entry<String, ArrayList<String>> entry : map.entrySet()) {
//	    	logger.info("entry map"+entry.getValue());
	    	ps.setString(1, entry.getValue().get(0));          
			ps.setString(2, entry.getValue().get(1));
			ps.setString(3, entry.getValue().get(2));
			ps.setString(4, entry.getValue().get(3));
			ps.setLong(5, Long.parseLong(entry.getValue().get(4))); 
			ps.setString(6, entry.getValue().get(5));
			ps.setString(7, entry.getValue().get(6));
	        ps.addBatch();//再添加一次预定义参数
	        map.remove(entry.getKey());
	       }
	        ps.executeBatch();//执行批量执行  
	        conn.commit();        
	        ps.close();    
	        conn.close();
//	        map.clear();
//	        map.clear();
	        LastTran5min =System.currentTimeMillis(); 
	        logger.info("suceess flush mysql map count "+flushSize);
		} catch (SQLException e) {
			// TODO Auto-generated catch block			
			logger.error("map has problem:"+map+e.getMessage());
			e.printStackTrace();
		} 
        }
		
        
        long deta = System.currentTimeMillis()- LastTran1hour-(15*60*1000);
//    	控制每次发送数据间隔在大于1s,小于15min
        if (deta>0){
        	if(size1hour>20){
          	size1hour=size1hour-10;
          	logger.info("mysql desc size1hour:"+size1hour);
        	}
            }
	      if (mapDomain.size()>size1hour){
	    	  long fqu1hour = (System.currentTimeMillis()-LastTran1hour)/1000;
	          if (fqu1hour<1){
	        	size1hour=size1hour+50;
	        	logger.info("mysql add size1hour:"+size1hour);
	          }
	    		try {
	    			Connection conn = DriverManager.getConnection(URL, user, pass);  
	    	        conn.setAutoCommit(false);  
	    	        //向mysql中插入数据  
	    	        String sql = "insert into AreaStat(counterName,area,type,time_stamp,count,stat,domain)values(?,?,?,?,?,?,?)";  
	    	        PreparedStatement ps = conn.prepareStatement(sql);//要执行sql语句的对象  
	    	        int flushSize = mapDomain.size();
	    	        for (Entry<String, ArrayList<String>> entry1 : mapDomain.entrySet()) {
	    	    	ps.setString(1, entry1.getValue().get(0));          
	    			ps.setString(2, entry1.getValue().get(1));
	    			ps.setString(3, entry1.getValue().get(2));
	    			ps.setString(4, entry1.getValue().get(3));
	    			ps.setLong(5, Long.parseLong(entry1.getValue().get(4)));
	    			ps.setString(6, entry1.getValue().get(5));
	    			ps.setString(7, entry1.getValue().get(6));  	        
	    	        ps.addBatch();//再添加一次预定义参数
	    	        mapDomain.remove(entry1.getKey());
	    	       }
	    	        ps.executeBatch();//执行批量执行  
	    	        conn.commit();         
	    	        ps.close();  
	    	        conn.close();  	    	        
//	    	        mapDomain.clear();
	    	        LastTran1hour=System.currentTimeMillis(); 
	    	        logger.info("suceess flush mysql mapDomain count "+flushSize);
	    		}
	        	        
		 catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();	
//			return;
		}
	      }}  } 
	@Override
	public void close() {
		// TODO Auto-generated method stub
		if (conn != null) {  
            try {
            	conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
        } 
	}
}
