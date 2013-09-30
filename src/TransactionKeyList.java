import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;

public class TransactionKeyList extends Key implements HasParser {
	// Identifier hash associated with nodes
	HashSet<String> uniqueKeys = new HashSet<String>();
	private String textKey;
	private String table = "transactionkey_list";
	private Connection connection;
	private final String file = "transactionkey_list.txt";

	public void readDataFile() {
		try {
			connection = Database.get().connectPostgre();
			emptyTable();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new DataInputStream(new FileInputStream(HasParser.path
							+ file))));
			String insertTableSQL = "INSERT INTO " + table
					+ "(id, transaction_key_string) VALUES" + "(?,?)";
			PreparedStatement preparedStatement = connection
					.prepareStatement(insertTableSQL);
			String strLine;
			int rows = 0;
			while ((strLine = br.readLine()) != null) {
				textKey = String.valueOf(strLine);

				preparedStatement.setInt(1, rows + 1);
				preparedStatement.setString(2, String.valueOf(strLine));

				if (!uniqueKeys.add(textKey)) {
					System.out.println("Double entry!" + (rows + 1) + ":"
							+ textKey);
				}

				preparedStatement.addBatch();
				// execute insert SQL stetement
				// preparedStatement .executeUpdate();
				if (rows % batchSize == 0) {
					System.out.println("Try to Insert at " + new Date());
					preparedStatement.executeBatch();
					System.out.println("Inserted " + rows + " at " + new Date());
					System.out
							.println("--------------------------------------------");
				}
				rows++;
			}
			preparedStatement.executeBatch();
			System.out.println(rows + " Records added");
			System.out.println(rows - uniqueKeys.size() + " Not unique keys");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			// System.err.println("Error: " + e.getMessage());
		}
	}

	@Override
	public void emptyTable() {
		String insertTableSQL = "DELETE FROM " + table;
		try {
			PreparedStatement preparedStatement = connection
					.prepareStatement(insertTableSQL);
			preparedStatement.addBatch();
			preparedStatement.executeBatch();
			System.out.println("Deleted all data from " + table + " at "
					+ new Date());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
