import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;


public class PublicKeyList extends Key{
	// Identifier hash associated with nodes
	HashSet<String> uniqueKeys = new HashSet<String>();
 	private final String file = "pubkey_list.txt";
	private String textKey;
	private Connection connection;
	
	public PublicKeyList(){
		table = "pubkey_list";
	}
	
	public void readDataFile() {
		try {
			connection = Database.get().connectPostgre();
			if(Config.isDBIntegration()){
				emptyTable();
			}
		    br = new BufferedReader(new InputStreamReader(
					new DataInputStream(new FileInputStream(HasParser.path
							+ file))));
			String insertTableSQL = "INSERT INTO "+table
					+ "(id, text_key) VALUES" + "(?,?)";
			preparedStatement = connection.prepareStatement(insertTableSQL);
			String strLine;
			int rows = 0;
			while ((strLine = br.readLine()) != null) {
				textKey = String.valueOf(strLine);
				
				if(Config.isDBIntegration()){
					preparedStatement.setInt(1, rows+1);
					preparedStatement.setString(2, String.valueOf(strLine));
					preparedStatement.addBatch();
				}		
				if(!uniqueKeys.add(textKey)){
					System.out.println("Double entry!"+(rows+1)+":"+textKey);
				}
				if (rows % batchSize == 0) {
					if(Config.isDBIntegration()){
						addToDatabase(rows, preparedStatement);
					}
				}
				rows++;
			}
			if(Config.isDBIntegration()){
				preparedStatement.executeBatch();
			}
			System.out.println(rows+" Records added");
			System.out.println(rows-uniqueKeys.size()+" Not unique keys");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			// System.err.println("Error: " + e.getMessage());
		}
	}

}
