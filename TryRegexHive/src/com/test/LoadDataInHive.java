package com.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class LoadDataInHive {

	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	static final String LOAD_TABLE = "LOAD DATA LOCAL INPATH '/home/chakrabt/logAnalysis/hiveTest.txt' OVERWRITE INTO TABLE testtab";
	
	public static void main(String[] args) {
		Connection con = null;
		Statement stmt = null;
		try{
			Class.forName(driverName);
			con = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");
			stmt = con.createStatement();
			stmt.execute(LOAD_TABLE);
			
			if(stmt != null){
				stmt.close();
			}
			if(con != null){
				con.close();
			}
		}catch(Exception ex){
			System.out.println(" Problem in Connecting Hive");
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
