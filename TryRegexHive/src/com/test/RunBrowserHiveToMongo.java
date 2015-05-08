package com.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class RunBrowserHiveToMongo {
	
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	static String LOAD_TABLE = "LOAD DATA LOCAL INPATH '/home/chakrabt/accesslog/samplelogfile' OVERWRITE INTO TABLE test_log_prod";
	static String TEMP_FUNC1 = "add jar /home/chakrabt/hiveCustomJar/ParseBrowserInfoUDF.jar";
	static String TEMP_FUNC = "CREATE TEMPORARY FUNCTION parseBrowserInfoUDF AS 'ParseBrowserInfoUDF'";
	static String GET_BROWSER_GROUP = "select parseBrowserInfoUDF(agent, 'browserName'),parseBrowserInfoUDF(agent, 'browserDt'), count(parseBrowserInfoUDF(agent, 'browserDt')) from test_log_prod group by parseBrowserInfoUDF(agent, 'browserName'), parseBrowserInfoUDF(agent, 'browserDt')";
	public static void main(String[] args) {
		
		Connection con = null;
		Statement stmt = null;
		Statement stmt1 = null;
		ResultSet rs1 = null;
		
		String ip = "localhost";
		int port = 27017;
		String db = "test";
		DB dataBase = null;
		String writeCollection = "TestBrowserCountInfo";
		
		try{
			String fileName = args[0];
			if(fileName != null && !("").equals(fileName)){
				LOAD_TABLE = LOAD_TABLE.replace("samplelogfile", fileName);
				System.out.println(" Modified LoadTable : "+LOAD_TABLE);
				
				dataBase = connectMongo(ip, port, db);
				DBCollection insertTable = dataBase.getCollection(writeCollection);
				
				Class.forName(driverName);
				con = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");
				stmt = con.createStatement();
				stmt.execute(TEMP_FUNC1);
				stmt.execute(TEMP_FUNC);
				stmt.execute(LOAD_TABLE);
				
				if(stmt != null){
					stmt.close();
				}
				
				stmt1 = con.createStatement();
				rs1 = stmt1.executeQuery(GET_BROWSER_GROUP);
				
				while(rs1.next()){
					String browserName = rs1.getString(1);
					String browserDt = rs1.getString(2);
					long browserDtCount = rs1.getLong(3);
					System.out.println(" browserName : "+browserName+" , browserDt : "+browserDt+" , browserDtCount : "+browserDtCount);
					if(browserName != null && !("").equals(browserName)){
						Long exsistingValue = getMongoValue(insertTable, browserName, browserDt);
						
						if(exsistingValue > 0){
							Long totalcount = exsistingValue + browserDtCount;
							
							BasicDBObject query = new BasicDBObject();
							query.put("browserName", browserName);
							query.put("browserDetail", browserDt);
							BasicDBObject newDocument = new BasicDBObject();
							newDocument.put("browserName", browserName);
							newDocument.put("browserDetail", browserDt);
							newDocument.put("browserDtCount", totalcount);
							insertTable.update(query, newDocument);
							System.out.println("IN Main UPDATE for browserName  : "+browserName+" , browserDt : "+browserDt+" , BrowserCount : "+browserDtCount+" , exsistingValue : "+exsistingValue+",totalcount : "+totalcount);
						}else{
							BasicDBObject document = new BasicDBObject();
							document.put("browserName", browserName);
							document.put("browserDetail", browserDt);
							document.put("browserDtCount", browserDtCount);
							insertTable.insert(document);
							System.out.println(" Main INSERT for browserName : "+browserName+" , browserDetail : "+browserDt+" , browserDtCount : "+browserDtCount);
						}
					}
				}
				
				if(stmt1 != null){
					stmt1.close();
				}
				if(rs1 != null){
					rs1.close();
				}
			}else{
				throw new Exception("Please provide the filename to load in HIVE in argument.");
			}
		}catch(Exception ex){
			System.out.println(" Problem in connection Hive"+ex.getMessage());
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
	
	
	public static DB connectMongo(String ip, int port, String db)
	throws Exception{
		Mongo mongo = new Mongo(ip, port);
		DB database = mongo.getDB(db);
		return database;
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
