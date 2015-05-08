
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ConnectHive {
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
	public static void main(String[] args) {
		try{
			Class.forName(driverName);
			Connection con = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");
			Statement stmt = con.createStatement();
			String sql = "show tables";
			ResultSet rs = stmt.executeQuery(sql);
			while(rs.next()){
				System.out.println(" Table : "+rs.getString(1)); ;
			}
		}catch(Exception ex){
			System.out.println(" Problem in getting driver");
			ex.printStackTrace();
		}
	}
	
}
