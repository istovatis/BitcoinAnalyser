package database;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBConnection {
	private final static String name = "abe";
	private final static String user = "postgres";
	private final static String pass = "elephant";
	public final static String database = "abe";
	private final static int port = 3306;
	
	/**
	 * The singleton instance of the class.
	 */
	private static DBConnection instance;

	public static DBConnection get() {
		if (instance == null) {
			instance = new DBConnection();
		}
		return instance;
	}
	
	public Connection getConnection() throws SQLException {

		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", user);
		connectionProps.put("password", pass);

		conn = DriverManager.getConnection("jdbc:mysql://" + "localhost:"
				+ port + "/"+name, connectionProps);

		System.out.println("Connected to database");
		return conn;
	}
	
	public Connection connectPostgre() {
		try {

			Class.forName("org.postgresql.Driver");

		} catch (ClassNotFoundException e) {

			System.out.println("Where is your PostgreSQL JDBC Driver? "
					+ "Include in your library path!");
			e.printStackTrace();
		}
		// System.out.println("PostgreSQL JDBC Driver Registered!");
		return connect();
	}
	
	public Connection connect() {
		Connection connection = null;
		try {
			connection = DriverManager
					.getConnection("jdbc:postgresql://127.0.0.1:5432/"+database,
							user, pass);
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
		}
		return connection;
	}
	

}
