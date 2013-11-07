import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Statement;
import java.sql.Connection;

public class UserPubKey  extends HasParser implements User {
	// Public keys used for input in "this" transaction
	private int[] keys;
	private final String file = "user_edge_input_public_keys.txt";
	private int rows;
	private int maxKeys;

	public void readDataFile() {
		try {
			connection = Database.get().connectPostgre();
			Statement st =connection.createStatement();
			br = new BufferedReader(new InputStreamReader(
					new DataInputStream(new FileInputStream(HasParser.path+file))));
			String strLine;

			while ((strLine = br.readLine()) != null) {
				rows++;
				String[] tags = strLine.split(",");
				int numKeys = tags.length-1;
				if(numKeys>maxKeys){
					maxKeys = numKeys;
				}
//				for(String tag:tags){
//					System.out.println(tag);
//				}
			}
			System.out.println("Rows: "+rows);
			System.out.println("MaxKeys: "+maxKeys);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			// System.err.println("Error: " + e.getMessage());
		}
	}
}
