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
 * Refers to the public keys that are used to every transaction.
 * For example, row 10819200,8862117,8862117 shows that for transaction
 * 10819200 two public keys are used:8862117 and 8862117.
 * @author istovatis
 *
 */
public class UserEdgeInputsPublicKeys extends UserEdges implements User, HasParser{
	// Transaction keys used as input in "this" transaction
	private int[] keys;
	private int transaction_key;
	private String table = "user_edge_inputs_public_keys";
	private final String file = "user_edge_inputs_public_keys.txt";
	private Connection connection;
	
	@Override
	public void readDataFile() {
		try {
			connection = Database.get().connectPostgre();
			emptyTable();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new DataInputStream(new FileInputStream(HasParser.path
							+ file))));
			String insertTableSQL = "INSERT INTO " + table
					+ "(transaction_key, public_keys) VALUES" + "(?,?)";
			PreparedStatement preparedStatement = connection
					.prepareStatement(insertTableSQL);
			String strLine;
			int rows = 0;
			while ((strLine = br.readLine()) != null) {
				String[] tags = strLine.split(",");
				transaction_key = Integer.valueOf(tags[0]);
				Integer[] key = new Integer[tags.length];
				if(tags.length>1){
					for(int i=1; i<tags.length; i++){
						key[i] = Integer.valueOf(tags[i]);
					}
				}	
								
				Array keys = connection.createArrayOf("int4", key);

				preparedStatement.setInt(1, rows + 1);
				preparedStatement.setArray(2, keys);

//				if (!uniqueKeys.add(key)) {
//					System.out.println("Double entry!" + (rows + 1) + ":"
//							+ key.toString());
//				}

				preparedStatement.addBatch();
				// execute insert SQL stetement
				// preparedStatement .executeUpdate();
				if (rows % batchSize == 0) {
					System.out.println("Try to Insert at " + new Date());
					preparedStatement.executeBatch();
					System.out
							.println("Inserted " + rows + " at " + new Date());
					System.out
							.println("--------------------------------------------");
				}
				rows++;
			}
			preparedStatement.executeBatch();
			System.out.println(rows + " Records added");
			//System.out.println(rows - uniqueKeys.size() + " Doublicate keys");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			// System.err.println("Error: " + e.getMessage());
		}
	}

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