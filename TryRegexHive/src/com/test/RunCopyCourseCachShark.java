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

public class RunCopyCourseCachShark {
	
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
	static String LOAD_TABLE_DATA = "LOAD DATA LOCAL INPATH '/home/chakrabt/logAnalysis/src/output_part/COPY_COURSE' INTO TABLE copycourse";
	static String DROP_CACHE = "DROP table copycourse_cached";
	static String CREATE_CACHE = "CREATE TABLE copycourse_cached AS SELECT * FROM copycourse";
	public static void main(String[] args) {
		
		Connection con = null;
		Statement stmt = null;
		try{
			Class.forName(driverName);
			con = DriverManager.getConnection("jdbc:hive://localhost:12000/default", "", "");
			long startTime = System.currentTimeMillis();
			stmt = con.createStatement();
			stmt.execute(LOAD_TABLE_DATA);
			stmt.execute(DROP_CACHE);
			stmt.execute(CREATE_CACHE);
			long endTime = System.currentTimeMillis();
			System.out.println(" Total Time taken to LOAD the data and Create the Cache copycourse : "+(endTime - startTime)+" ms.");
			
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
