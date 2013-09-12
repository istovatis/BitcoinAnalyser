import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.mysql.jdbc.PreparedStatement;


public class Database {
	private final static String name = "bitcoin";
	private final static String user = "root";
	private final static String pass = "1234";
	
	Connection c;
	Statement statement;	// statement for insert query
	PreparedStatement preparedStatement;	// statement for update query
	
	String query;	
	
	public static Connection getConnection() throws SQLException {

		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", user);
		connectionProps.put("password", pass);

		conn = DriverManager.getConnection("jdbc:mysql://" + "localhost:"
				+ 3306 + "/"+name, connectionProps);

		System.out.println("Connected to database");
		return conn;
	}

}

