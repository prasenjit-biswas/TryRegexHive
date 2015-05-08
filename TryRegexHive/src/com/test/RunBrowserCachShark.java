package com.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class RunBrowserCachShark {
	
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
	static String LOAD_TABLE_DATA = "LOAD DATA LOCAL INPATH '/home/chakrabt/accesslog/SampleFileName' INTO TABLE test_log_prod";
	static String DROP_CACHE = "DROP table test_log_prod_cached";
	static String CREATE_CACHE = "CREATE TABLE test_log_prod_cached AS SELECT * FROM test_log_prod";
	public static void main(String[] args) {
		
		Connection con = null;
		Statement stmt = null;
		try{
			String fileName = args[0];
			System.out.println(" fileName : "+fileName);
			LOAD_TABLE_DATA = LOAD_TABLE_DATA.replace("SampleFileName", fileName);
			System.out.println(" LOAD_TABLE_DATA : "+LOAD_TABLE_DATA);
			
			Class.forName(driverName);
			con = DriverManager.getConnection("jdbc:hive://localhost:12000/default", "", "");
			long startTime = System.currentTimeMillis();
			stmt = con.createStatement();
			stmt.execute(LOAD_TABLE_DATA);
			stmt.execute(DROP_CACHE);
			stmt.execute(CREATE_CACHE);
			long endTime = System.currentTimeMillis();
			System.out.println(" Total Time taken to LOAD the data and Create the Cache test_log_prod : "+(endTime - startTime)+" ms.");
			if(stmt != null){
				stmt.close();
			}
			if(con != null){
				con.close();
			}
		}catch(Exception ex){
			System.out.println(" Problem in connection Hive");
			ex.printStackTrace();
		}finally{
			try{
				if(stmt != null){
					stmt.close();
				}
				if(con != null){
					con.close();
				}
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
	}
}
