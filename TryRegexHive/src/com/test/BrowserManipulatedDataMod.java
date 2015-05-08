//package com.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class BrowserManipulatedDataMod {
	
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	//static String GET_BROWSER_QUERY = "SELECT browsername, browserdetail, browserdetailcount FROM browsermanipulatedata";
	static String GET_BROWSER_QUERY = "SELECT browsername, browserdetail, browserdetailcount FROM testbrowserManipulatedata";
	
	public static void main(String[] args) {
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		try{
			Mongo mongo = new Mongo("localhost", 27017);
			DB db = mongo.getDB("test");
			//DBCollection browserCountInfo = db.getCollection("BrowserCountInfo");
			DBCollection browserCountInfo = db.getCollection("TestBrowserCountInfo");
			Class.forName(driverName);
			con = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");
			stmt = con.createStatement();
			rs = stmt.executeQuery(GET_BROWSER_QUERY);
			while(rs.next()){
				String browserName = rs.getString(1);
				String browserDt = rs.getString(2);
				String browserDtCount = rs.getString(3);
				//System.out.println(" browserName : "+browserName+" , browserDt : "+browserDt+" ,browserDtCount : "+browserDtCount);
				
				Long exsistingValue = getMongoValue(browserCountInfo, browserName, browserDt);
				if(exsistingValue > 0){
					Long totalcount = exsistingValue + Long.parseLong(browserDtCount);
					browserDtCount = String.valueOf(totalcount);
					BasicDBObject query = new BasicDBObject();
					query.put("browserName", browserName);
					query.put("browserDetail", browserDt);
					BasicDBObject newDocument = new BasicDBObject();
					newDocument.put("browserName", browserName);
					newDocument.put("browserDetail", browserDt);
					newDocument.put("browserDtCount", browserDtCount);
					browserCountInfo.update(query, newDocument);
					System.out.println(" IN Main for browserName : "+browserName+" , browserDt : "+browserDt+" , BrowserCount : "+browserDtCount+" , exsistingValue : "+exsistingValue+",totalcount : "+totalcount);
				}else{
					String mongoValue = "{\"browserName\":"+"\""+browserName+"\""+",\"browserDetail\":"+"\""+browserDt+"\""+",\"browserDtCount\":"+"\""+browserDtCount+"\""+"}";
					System.out.println(" mongoValue : "+mongoValue);
					Object o = com.mongodb.util.JSON.parse(mongoValue);
					DBObject dbObj = (DBObject) o;
					browserCountInfo.save(dbObj);
				}
			}
		}catch(Exception ex){
			System.out.println(" Problem in connection Hive");
			ex.printStackTrace();
		}finally{
			try{
				if(con != null){
					con.close();
				}
				if(stmt != null){
					stmt.close();
				}
				if(rs != null){
					rs.close();
				}
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
	}
	
	private static Long getMongoValue(DBCollection browserCountInfo, String browserName, String browserDt) throws Exception{
		Long returnCount = 0l;
		BasicDBObject query = new BasicDBObject();
		query.put("browserName", browserName);
		query.put("browserDetail", browserDt);
		try{
			DBCursor cursor = browserCountInfo.find(query);
			if(cursor.hasNext()){
				BasicDBObject obj = (BasicDBObject) cursor.next();
			    String countStr = obj.getString("browserDtCount");
			    returnCount = Long.parseLong(countStr);
			}
		}catch(Exception e){
			System.out.println(" Problem in getMongoValue for browserName : "+browserName+" , browserDt : "+browserDt);
			e.printStackTrace();
		}
		System.out.println(" IN getMongoValue for browserName : "+browserName+" , browserDt : "+browserDt+" , exsistingBrowserCount : "+returnCount);
		return returnCount;
	}
}
