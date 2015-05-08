//package com.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class RunAgentGroupSharkCache {
	
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
	static String DROP_CACHE = "DROP table test_log_prod_cached";
	static String CREATE_CACHE = "CREATE TABLE test_log_prod_cached AS SELECT * FROM test_log_prod";
	static String TEMP_FUNC1 = "add jar /home/chakrabt/hiveCustomJar/ParseBrowserInfoUDF.jar";
	static String TEMP_FUNC = "CREATE TEMPORARY FUNCTION parseBrowserInfoUDF AS 'ParseBrowserInfoUDF'";
	static String GET_BROWSER_GROUP = "select parseBrowserInfoUDF(agent, 'browserName'),parseBrowserInfoUDF(agent, 'browserDt'), count(parseBrowserInfoUDF(agent, 'browserDt')) from test_log_prod_cached group by parseBrowserInfoUDF(agent, 'browserName'), parseBrowserInfoUDF(agent, 'browserDt')";
	public static void main(String[] args) {
		
		Connection con = null;
		Statement stmt = null;
		Statement stmt1 = null;
		ResultSet rs1 = null;
		try{
			Class.forName(driverName);
			con = DriverManager.getConnection("jdbc:hive://localhost:12000/default", "", "");
			
			stmt = con.createStatement();
			stmt.execute(DROP_CACHE);
			stmt.execute(CREATE_CACHE);
			stmt.execute(TEMP_FUNC1);
			stmt.execute(TEMP_FUNC);
			
			if(stmt != null){
				stmt.close();
			}
			
			long startTime = System.currentTimeMillis();
			stmt1 = con.createStatement();
			rs1 = stmt1.executeQuery(GET_BROWSER_GROUP);
			
			while(rs1.next()){
				String browserName = rs1.getString(1);
				String browserDt = rs1.getString(2);
				long browserDtCount = rs1.getLong(3);
				System.out.println(" browserName : "+browserName+" , browserDt : "+browserDt+" , browserDtCount : "+browserDtCount);
			}
			long endTime = System.currentTimeMillis();
			System.out.println(" Total Time taken to get the data : "+(endTime - startTime)+" ms.");
			if(stmt1 != null){
				stmt1.close();
			}
			if(rs1 != null){
				rs1.close();
			}
		}catch(Exception ex){
			System.out.println(" Problem in connection Hive");
			ex.printStackTrace();
		}finally{
			try{
				if(stmt != null){
					stmt.close();
				}
				if(stmt1 != null){
					stmt1.close();
				}
				if(rs1 != null){
					rs1.close();
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
