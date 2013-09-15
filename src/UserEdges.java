import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * This class keeps all transaction data
 * 
 * @author istovatis
 * 
 */
public class UserEdges implements HasParser{
	protected int transactionKey;
	private int userFromKey;
	private int userKeyTo;
	private int date;
	private int time;
	private float value;

	private final String file = "user_edges.txt";
	SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
	SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("hh:mm:ss");

	public void readDataFile() {
		try {
			Connection connection = Database.get().connectPostgre();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new DataInputStream(new FileInputStream(HasParser.path
							+ file))));
			String insertTableSQL = "INSERT INTO user_edges"
					+ "(transaction_key, user_from_key, user_key_to, date, value) VALUES"
					+ "(?,?,?,?,?)";
			PreparedStatement preparedStatement = connection.prepareStatement(insertTableSQL);
			String strLine;
			int rows = 0;
			while ((strLine = br.readLine()) != null) {
				String[] tags = strLine.split(",");	
				preparedStatement.setInt(1, Integer.valueOf(tags[0]));
				preparedStatement.setInt(2, Integer.valueOf(tags[1]));
				preparedStatement.setInt(3, Integer.valueOf(tags[2]));
				preparedStatement.setTimestamp(4,getCurrentTimeStamp(returnDate(tags[3])));
				preparedStatement.setDouble(5, Double.valueOf(tags[4]));
				preparedStatement.addBatch();
				// execute insert SQL stetement
				//preparedStatement .executeUpdate();
				if (rows % batchSize == 0) {
					System.out.println("Try to Insert at "+new Date());
					preparedStatement.executeBatch();
					System.out.println(rows+" Inserted at "+new Date());
					System.out.println("--------------------------------------------");
					}
				rows++;
			}
			preparedStatement.executeBatch();
			System.out.println(rows+" Records added");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			// System.err.println("Error: " + e.getMessage());
		}
	}
	
	public Date returnDate(String input){
		String year = input.substring(0, 4);
		year = year.concat("-");
		String month = input.substring(4, 6);
		month = month.concat("-");
		String day = input.substring(6, 8);
		String date = year+month+day;
		
		String hour = input.substring(8, 10);
		hour = hour.concat(":");
		String min = input.substring(10, 12);
		min = min.concat(":");
		String sec = input.substring(12, 14); 
		String time = hour+min+sec;
		date = date.concat("-"+time);
		Date d = null;
		try {
			d = DATE_FORMAT.parse(date);
			//System.out.println(">>"+DATE_FORMAT.format(d));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return d;
	}
	
	public Date returnTime(String input){
		String hour = input.substring(8, 10);
		hour = hour.concat(":");
		String min = input.substring(10, 12);
		min = min.concat(":");
		String sec = input.substring(12, 14); 
		String time = hour+min+sec;
		Date d = null;
		try {
			d = TIME_FORMAT.parse(time);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return d;
	}
	
	private static java.sql.Timestamp getCurrentTimeStamp(Date date) {
		return new java.sql.Timestamp(date.getTime());
	}
}
