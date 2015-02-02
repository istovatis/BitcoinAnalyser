package database;
import java.io.BufferedReader;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;


/**
 * Basic DB interaction layer
 * @author istovatis
 *
 */
public abstract class DBInteraction {
	protected String table;
	protected String duplicateTable;
	public static int batchSize = 500000;
	protected static PreparedStatement preparedStatement;
	protected static PreparedStatement duplicateStatement;  // Statement for dublicate data
	protected static Connection connection = DBConnection.get().connectPostgre();
	protected static BufferedReader br; // BufferReader for file parsing

	protected static String path = "Lists" + File.separator;

	public DBInteraction() {
		connection = DBConnection.get().connectPostgre();
	}

	public void readDataFile() {
		// Read data files
	}

	/**
	 * Empty the table from database. The table corresponds to the specific
	 * class
	 */
	public int emptyTable() {
		int recordsDeleted = 0;
		String insertTableSQL = "DELETE FROM " + this.table;
		try {
			PreparedStatement preparedStatement = this.connection
					.prepareStatement(insertTableSQL);
			preparedStatement.addBatch();
			recordsDeleted = preparedStatement.executeUpdate(); //.length;
			System.out.println("Deleted all data from " + table + " at "
					+ new Date());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return recordsDeleted;
	}
	
	/**
	 * Empty the table from database. The table corresponds to the specific
	 * class
	 */
	public int emptyTable(String table) {
		int recordsDeleted = 0;
		String insertTableSQL = "DELETE FROM " + table;
		try {
			PreparedStatement preparedStatement = this.connection.prepareStatement(insertTableSQL);
			preparedStatement.addBatch();
			recordsDeleted = preparedStatement.executeUpdate(); //.executeBatch();
			System.out.println("Deleted all data from " + table + " at " + new Date());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return recordsDeleted;
	}

	public void addToDatabase(int rows, PreparedStatement statement) {
		System.out.println("Try to Insert at " + new Date());
		try {
			statement.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("More info: "+e.getNextException());
		}
		System.out.println(rows + " Inserted at " + new Date());
		System.out.println("--------------------------------------------");
	}
}