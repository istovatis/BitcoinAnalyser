package bitcointools;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;

import database.DBConnection;
import database.DBInteraction;

import abe.Config;


public class TransactionKeyList extends Key {
	// Identifier hash associated with nodes
	HashSet<String> uniqueKeys = new HashSet<String>();
	private String textKey;
	private Connection connection;
	private final String file = "transactionkey_list.txt";
	
	public TransactionKeyList(){
		table = "transactionkey_list";
	}
	
	public void readDataFile() {
		try {
			connection = DBConnection.get().connectPostgre();
			if(Config.isDBIntegration()){
				emptyTable();
			}
			br = new BufferedReader(new InputStreamReader(
					new DataInputStream(new FileInputStream(DBInteraction.path
							+ file))));
			String insertTableSQL = "INSERT INTO " + table
					+ "(id, transaction_key_string) VALUES" + "(?,?)";
			preparedStatement = connection
					.prepareStatement(insertTableSQL);
			String strLine;
			int rows = 0;
			while ((strLine = br.readLine()) != null) {
				textKey = String.valueOf(strLine);

				preparedStatement.setInt(1, rows + 1);
				preparedStatement.setString(2, String.valueOf(strLine));
				if(Config.isDublicatesIntegration()){
					if (!uniqueKeys.add(textKey)) {
						System.out.println("Double entry!" + (rows + 1) + ":"
								+ textKey);
					}
				}
		
				preparedStatement.addBatch();
				// execute insert SQL stetement
				// preparedStatement .executeUpdate();
				if (rows % batchSize == 0) {
					if(Config.isDBIntegration()){
						addToDatabase(rows, preparedStatement);
					}
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

}
