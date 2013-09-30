import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;

public class UserKeyList implements HasParser {
	// public keys that belong to the same user
	private String table = "userkey_list";
	private Integer[] keys;
	//private ArrayList<Integer> keyList;
	private HashSet<Integer[]> uniqueKeys = new HashSet<Integer[]>(); 
	Connection connection; 
	
	private final String file = "userkey_list.txt";

	@Override
	public void readDataFile() {
		try {
			connection = Database.get().connectPostgre();
			if(Config.DBIntegration){
				emptyTable();
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new DataInputStream(new FileInputStream(HasParser.path
							+ file))));
			String insertTableSQL = "INSERT INTO " + table
					+ "(key_number, public_keys) VALUES" + "(?,?)";
			PreparedStatement preparedStatement = connection
					.prepareStatement(insertTableSQL);
			String strLine;
			int rows = 0;
			while ((strLine = br.readLine()) != null) { 
				String[] tags = strLine.split(",");
				Integer[] key = new Integer[tags.length];
				for(int i=0; i<tags.length; i++){
					key[i] = Integer.valueOf(tags[i]);
				}
								
				Array keys = connection.createArrayOf("int4", key);
				if(Config.DBIntegration){
					preparedStatement.setInt(1, rows + 1);
					preparedStatement.setArray(2, keys);
					preparedStatement.addBatch();
				}
				if (!uniqueKeys.add(key)) {
					System.out.println("Double entry!" + (rows + 1) + ":"
							+ key.toString());
				}
				if (rows % batchSize == 0) {
					System.out.println("Try to Insert at " + new Date());
					if(Config.DBIntegration){
						preparedStatement.executeBatch();
					}
					System.out
							.println("Inserted " + rows + " at " + new Date());
					System.out
							.println("--------------------------------------------");
				}
				rows++;
			}
			if(Config.DBIntegration){
				preparedStatement.executeBatch();
			}
			System.out.println(rows + " Records added");
			System.out.println(rows - uniqueKeys.size() + " Duplicate keys");
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
