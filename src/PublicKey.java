import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;


public class PublicKey extends Key implements HasParser{
	// Identifier hash associated with nodes
	HashSet<String> uniqueKeys = new HashSet<String>();
 	private final String file = "pubkey_list.txt";
	private String textKey;
	
	public void readDataFile() {
		try {
			Connection connection = Database.get().connectPostgre();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new DataInputStream(new FileInputStream(HasParser.path
							+ file))));
			String insertTableSQL = "INSERT INTO pubkey_list"
					+ "(id, text_key) VALUES" + "(?,?)";
			PreparedStatement preparedStatement = connection.prepareStatement(insertTableSQL);
			String strLine;
			int rows = 0;
			while ((strLine = br.readLine()) != null) {
				textKey = String.valueOf(strLine);

     			preparedStatement.setInt(1, rows+1);
				preparedStatement.setString(2, String.valueOf(strLine));

				if(!uniqueKeys.add(textKey)){
					System.out.println("Double entry!"+(rows+1)+":"+textKey);
				}
		
				preparedStatement.addBatch();
				// execute insert SQL stetement
				//preparedStatement .executeUpdate();
				if (rows % batchSize == 0){ 
					System.out.println("Try to Insert at "+new Date());
					preparedStatement.executeBatch();
					System.out.println("Inserted "+rows+ "at "+new Date());
					System.out.println("--------------------------------------------");
					}
				rows++;
			}
			preparedStatement.executeBatch();
			System.out.println(rows+" Records added");
			System.out.println(rows-uniqueKeys.size()+" Not unique keys");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			// System.err.println("Error: " + e.getMessage());
		}
	}
}
