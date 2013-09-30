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


public class PublicKeyList extends Key implements HasParser{
	// Identifier hash associated with nodes
	HashSet<String> uniqueKeys = new HashSet<String>();
 	private final String file = "pubkey_list.txt";
	private String textKey;
	private String table = "pubkey_list";
	private Connection connection;
	
	public void readDataFile() {
		try {
			connection = Database.get().connectPostgre();
			if(Config.DBIntegration){
				emptyTable();
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new DataInputStream(new FileInputStream(HasParser.path
							+ file))));
			String insertTableSQL = "INSERT INTO "+table
					+ "(id, text_key) VALUES" + "(?,?)";
			PreparedStatement preparedStatement = connection.prepareStatement(insertTableSQL);
			String strLine;
			int rows = 0;
			while ((strLine = br.readLine()) != null) {
				textKey = String.valueOf(strLine);
				
				if(Config.DBIntegration){
					preparedStatement.setInt(1, rows+1);
					preparedStatement.setString(2, String.valueOf(strLine));
					preparedStatement.addBatch();
				}		
				if(!uniqueKeys.add(textKey)){
					System.out.println("Double entry!"+(rows+1)+":"+textKey);
				}
				if (rows % batchSize == 0){ 
					System.out.println("Try to Insert at "+new Date());
					if(Config.DBIntegration){
						preparedStatement.executeBatch();
					}
					System.out.println("Inserted "+rows+ "at "+new Date());
					System.out.println("--------------------------------------------");
					}
				rows++;
			}
			if(Config.DBIntegration){
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

	@Override
	public void emptyTable() {
		String insertTableSQL = "DELETE FROM "+table;
		try {
			PreparedStatement preparedStatement = connection
					.prepareStatement(insertTableSQL);
			preparedStatement.addBatch();
			preparedStatement.executeBatch();
			System.out.println("Deleted all data from "+table+ " at "+new Date());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
