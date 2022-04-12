package mulesoftChallenge;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MoviesDB {

	public static void main(String[] args) {
		Connection conn= null;
		try {
			conn=DriverManager.getConnection("jdbc:sqlite:movies.db");
			System.out.println("connection established");
			
			try {
				deleteTable(conn);		//table must exist
			}
			catch(Exception ignored) {
				//table doesn't exist
			}
			
			createTable(conn);
			
			System.out.println();
			System.out.println("Inserting data");
			insertMovie(conn, "yjhd", "ranbir kapoor", "deepika padukone", 2013, "ayan mukherjee");
			insertMovie(conn, "znmd", "hrithik roshan", "katrina kaif", 2011, "zoya akhtar");
			insertMovie(conn, "dear zindagi", "sharukh khan", "alia bhatt", 2016, "gauri shinde");
			insertMovie(conn, "main tera hero", "varun dhawan", "illeana dcruz", 2014, "david dhawan");
			insertMovie(conn, "kapoor and sons", "siddharth malhotra", "alia", 2016, "shakun batra");
			
			System.out.println();
			System.out.println("Displaying database");
			displayDatabase(conn, "Movies");
		} catch(SQLException e) {
			e.printStackTrace();
			System.out.println(e.getClass().getName()+": "+ e.getMessage());
		}
		finally {
			if(conn!=null) {
				try {
					conn.close();
				} catch(SQLException e) {
					e.printStackTrace();
					System.out.println(e.getMessage());
				}
			}
		}
	}
	
	private static void displayDatabase(Connection conn, String tableName) throws SQLException{
		String selectSQL= "SELECT * from "+ tableName;
		Statement stmt= conn.createStatement();
		ResultSet rs= stmt.executeQuery(selectSQL);
		
		System.out.println("--------"+ tableName+"--------");
		while(rs.next()) {
			System.out.println("Movie: "+ rs.getString("title")+", ");
			System.out.println(rs.getString("actor")+ ", ");
			System.out.println(rs.getString("actress")+ ", ");
			System.out.println(rs.getInt("year")+", ");
			System.out.println(rs.getString("director")+ ", ");
		}
		System.out.println("----------------------------------");
	}
	
	private static void insertMovie(Connection conn, String title, String actor, String actress, int year, String director) throws SQLException{
		String insertSQL= "INSERT INTO Movies(title, actor, actress, year, director) VALUES(?,?,?,?,?)";
		PreparedStatement pstmt= conn.prepareStatement(insertSQL);
		pstmt.setString(1, title);
		pstmt.setString(2, actor);
		pstmt.setString(3, actress);
		pstmt.setInt(4, year);
		pstmt.setString(5, director);
		pstmt.executeUpdate();
	}
	
	private static void createTable(Connection conn) throws SQLException{
		String createTableSQL=""+
					"CREATE TABLE Movies" +
					"( "+
					"title varchar(255), "+
					"actor varchar(255), "+
					"actress varchar(255), "+
					"year integer, "+
					"director varchar(255), "+
					"); "+
					"";
		Statement stmt= conn.createStatement();
		stmt.execute(createTableSQL);
	}
	
	private static void deleteTable(Connection conn) throws SQLException{
		String deleteTableSQL= "DROP TABLE Movies";
		Statement stmt= conn.createStatement();
		stmt.execute(deleteTableSQL);
	}

}
