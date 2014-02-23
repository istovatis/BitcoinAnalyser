package bitcointools;
import java.io.BufferedReader;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

public abstract class HasParser {
	protected String table;
	protected String duplicateTable;
	public final static int batchSize = 500000;
	protected PreparedStatement preparedStatement;
	protected PreparedStatement duplicateStatement;  // Statement for dublicate data
	protected Connection connection;
	BufferedReader br; // BufferReader for file parsing
//	final static String path = System.getProperty("user.home")
//			+ File.separator
//			+ "Dropbox/development/bitcoin/bitcoin_uic_data_and_code_20130107/bitcoin_network_blockchain_215529/";
	//final static String path = File.separator+ "media/soloikos/723D3C603F1AF96D/development/bitcoin/bitcoin_uic_data_and_code_20130410/";
	final static String path = System.getProperty("user.home")+File.separator+"bitcoin"+File.separator;
	// System.getProperty("user.dir")+File.separator;
	public HasParser() {
		connection = Database.get().connectPostgre();
	}

	public abstract void readDataFile();

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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return recordsDeleted;
	}
	
	/**
	 * Empty the table from database. The table corresponds to the specific
	 * class
	 */
	public int emptyDuplicateTable() {
		int recordsDeleted = 0;
		String insertTableSQL = "DELETE FROM " + this.duplicateTable;
		try {
			PreparedStatement preparedStatement = this.connection
					.prepareStatement(insertTableSQL);
			preparedStatement.addBatch();
			recordsDeleted = preparedStatement.executeUpdate(); //.executeBatch();
			System.out.println("Deleted all data from " + this.duplicateTable + " at "
					+ new Date());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return recordsDeleted;
	}

	public void addToDatabase(int rows, PreparedStatement statement) {
		System.out.println("Try to Insert at " + new Date());
		try {
			statement.executeBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(rows + " Inserted at " + new Date());
		System.out.println("--------------------------------------------");
	}
}