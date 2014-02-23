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

/**
 * Refers to the public keys that are used to every transaction. For example,
 * row 10819200,8862117,8862117 shows that for transaction 10819200 two public
 * keys are used:8862117 and 8862117.
 * 
 * @author istovatis
 * 
 */
public class UserEdgeInputsPublicKeys extends HasParser implements User {
	// Transaction keys used as input in "this" transaction
	private int[] keys;
	private int transaction_key;
	private final String file = "user_edge_input_public_keys.txt";
	private Connection connection;

	public UserEdgeInputsPublicKeys() {
		table = "user_edge_inputs_public_keys";
	}

	public void readDataFile() {
		try {
			connection = Database.get().connectPostgre();
			if (Config.isDBIntegration()) {
				emptyTable();
			}
			br = new BufferedReader(new InputStreamReader(new DataInputStream(
					new FileInputStream(HasParser.path + file))));
			String insertTableSQL = "INSERT INTO " + table
					+ "(line, transaction_key, public_keys) VALUES" + "(?,?,?)";
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

					// if (!uniqueKeys.add(key)) {
					// System.out.println("Double entry!" + (rows + 1) + ":"
					// + key.toString());
					// }

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
			// System.out.println(rows - uniqueKeys.size() +
			// " Doublicate keys");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			// System.err.println("Error: " + e.getMessage());
		}
	}
}