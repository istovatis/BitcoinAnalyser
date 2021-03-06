package bitcointools;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

import database.DBConnection;
import database.DBInteraction;

import abe.Config;


/**
 * This class refers to Transaction keys used as input in "this" transaction.
 * For example at row: 46,626,639,640,641,624,627, for transaction id=46
 * transactions 626,639,640,641,624,627 are reffered as input.
 * 
 * @author istovatis
 * 
 */
public class UserEdgeInputs extends DBInteraction implements User {
	// Transaction keys used as input in "this" transaction
	private int[] keys;

	private int transaction_key;
	private final String file = "user_edge_inputs.txt";

	public UserEdgeInputs() {
		table = "user_edge_inputs";
	}

	@Override
	public void readDataFile() {
		try {
			connection = DBConnection.get().connectPostgre();
			if (Config.isDBIntegration()) {
				emptyTable();
			}
			br = new BufferedReader(new InputStreamReader(new DataInputStream(
					new FileInputStream(DBInteraction.path + file))));
			String insertTableSQL = "INSERT INTO " + table
					+ "(line, transaction_key, transaction_keys) VALUES"
					+ "(?,?,?)";
			preparedStatement = connection.prepareStatement(insertTableSQL);
			String strLine;
			int rows = 0;
			while ((strLine = br.readLine()) != null) {
				String[] tags = strLine.split(",");
				transaction_key = Integer.valueOf(tags[0]);
				int pubKeys = 0;
				if (tags.length > 1) {
					pubKeys = tags.length - 1;
				}
				Integer[] key = new Integer[pubKeys];

				for (int i = 0; i < pubKeys; i++) {
					key[i] = Integer.valueOf(tags[i + 1]);
				}

				Array keys = connection.createArrayOf("int4", key);
				if (Config.isDBIntegration()) {
					preparedStatement.setInt(1, rows + 1);
					preparedStatement.setInt(2, transaction_key);
					preparedStatement.setArray(3, keys);

					preparedStatement.addBatch();
					// execute insert SQL stetement
					// preparedStatement .executeUpdate();
					if (rows % batchSize == 0) {
						if (Config.isDBIntegration()) {
							addToDatabase(rows, preparedStatement);
						}
					}
				}
				rows++;
			}
			preparedStatement.executeBatch();
			System.out.println(rows + " Records added");
	
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}
}
