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

public class ApacheLogAgentGroup {
	
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	static String AgentGroupByQuery = "SELECT agent, count(*) AS agentCount FROM test_log group by agent";
	
	public static void main(String[] args) {
		try{
			Mongo mongo = new Mongo("localhost", 27017);
			DB db = mongo.getDB("test");
			DBCollection browserCountInfo = db.getCollection("BrowserCountInfo");
			Class.forName(driverName);
			Connection con = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(AgentGroupByQuery);
			while(rs.next()){
				String agent = rs.getString(1);
				String agentCount = rs.getString(2);
				System.out.println(" agent : "+agent+" , agentCount : "+agentCount);
				DBObject query = new BasicDBObject();
			    query.put("_id", agent);
			    query.put("value", agentCount);
			    browserCountInfo.save(query);
			}
		}catch(Exception ex){
			System.out.println(" Problem in connection Hive");
			ex.printStackTrace();
		}
	}
}
