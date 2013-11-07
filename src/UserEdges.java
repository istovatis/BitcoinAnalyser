import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.collections.MultiHashMap;
import org.apache.commons.collections.MultiMap;

/**
 * This class keeps all transaction data
 * 
 * @author istovatis
 * 
 */
public class UserEdges extends HasParser {
	protected int transactionKey;
	private int from;
	private int to;
	private int date;
	private Timestamp time;
	private double value;
	private int line;	//line number at parsed file
	
	public UserEdges(){
		table = "user_edges";
		duplicateTable = "user_edges_duplicates";
	}

	private final String file = "user_edges.txt";
	MultiMap mhm = new MultiHashMap();
	SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
	SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("hh:mm:ss");
	HashSet<ArrayList<Number>> uniqueKey = new HashSet<ArrayList<Number>>();
	HashSet<ArrayList<Number>> dublicateKey = new HashSet<ArrayList<Number>>();		// Set of dublicate keys

	// MultiMap uniqueKey = new MultiHashMap();

	public void readDataFile() {
		try {
			br = new BufferedReader(new InputStreamReader(
					new DataInputStream(new FileInputStream(HasParser.path
							+ file))));
			String insertTableSQL = "INSERT INTO "
					+ table
					+ "(line, transaction_key, user_from_key, user_key_to, date, value) VALUES"
					+ "(?,?,?,?,?,?)";
			String insertDublicateSQL = "INSERT INTO "
					+ duplicateTable
					+ "(line, transaction_key, user_from_key, user_key_to, date, value) VALUES"
					+ "(?,?,?,?,?,?)";
			preparedStatement = connection
					.prepareStatement(insertTableSQL);
			duplicateStatement = connection
					.prepareStatement(insertDublicateSQL);
			
			String strLine;
			int rows = 0;
			if(Config.isDBIntegration()){
				System.out.println(emptyTable()+" Records deleted from "+table);
			}
			if(Config.isDublicatesIntegration()){
				System.out.println(emptyDuplicateTable()+" Records deleted from "+this.duplicateTable);
			}
			while ((strLine = br.readLine()) != null) {
				String[] tags = strLine.split(",");
				line = rows+1;
				transactionKey = Integer.valueOf(tags[0]);
				from = Integer.valueOf(tags[1]);
				to = Integer.valueOf(tags[2]);
				value = Double.valueOf(tags[4]);
				time = getCurrentTimeStamp(returnDate(tags[3]));
				preparedStatement.setInt(1, Integer.valueOf(line));
				preparedStatement.setInt(2, Integer.valueOf(tags[0]));
				preparedStatement.setInt(3, Integer.valueOf(tags[1]));
				preparedStatement.setInt(4, Integer.valueOf(tags[2]));
				preparedStatement.setTimestamp(5,
						getCurrentTimeStamp(returnDate(tags[3])));
				preparedStatement.setDouble(6, Double.valueOf(tags[4]));
				// int []tmp = {key, from, to};
				// uniqueKey.add(Arrays.hashCode(tmp));
				if(Config.isDBIntegration()){
					// execute insert SQL stetement
					preparedStatement.addBatch();	
				}
				//if(Config.isDublicatesIntegration()){
					checkForPublicates();
				//}
				if (rows % batchSize == 0) {
					System.out.println("Records:"+(rows + 1)+" at "+new Date());
					if(Config.isDBIntegration()){
						addToDatabase(rows, preparedStatement);
						System.out.println((rows + 1) - uniqueKey.size()
								+ " Duplicate keys");
					}
					if(Config.isDublicatesIntegration()){
						addToDatabase(rows, duplicateStatement);
					}
				}
				rows++;
			}
			preparedStatement.executeBatch();
			duplicateStatement.executeBatch();
			System.out.println(rows + " Records added");
			System.out.println(rows - uniqueKey.size() + " Not unique keys");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			// System.err.println("Error: " + e.getMessage());
		}
	}

	public Date returnDate(String input) {
		String year = input.substring(0, 4);
		year = year.concat("-");
		String month = input.substring(4, 6);
		month = month.concat("-");
		String day = input.substring(6, 8);
		String date = year + month + day;

		String hour = input.substring(8, 10);
		hour = hour.concat(":");
		String min = input.substring(10, 12);
		min = min.concat(":");
		String sec = input.substring(12, 14);
		String time = hour + min + sec;
		date = date.concat("-" + time);
		Date d = null;
		try {
			d = DATE_FORMAT.parse(date);
			// System.out.println(">>"+DATE_FORMAT.format(d));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return d;
	}

	public Date returnTime(String input) {
		String hour = input.substring(8, 10);
		hour = hour.concat(":");
		String min = input.substring(10, 12);
		min = min.concat(":");
		String sec = input.substring(12, 14);
		String time = hour + min + sec;
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

	public void checkForPublicates() {
		ArrayList<Number> tmp2 = new ArrayList<Number>();
		tmp2.add(transactionKey);
		tmp2.add(from);
		tmp2.add(to);
		tmp2.add(value);
		tmp2.add(time.getTime());
		if (!uniqueKey.add(tmp2)) {
//			System.out.println("Double entry! "+ line+" "+ transactionKey + "  " + from
//					+ "  " + to + "  " + value + "  " + time);
			if(Config.isDublicatesIntegration()){
				//if(dublicateKey.add(tmp2)){
					try {
						duplicateStatement.setInt(1, Integer.valueOf(line));
						duplicateStatement.setInt(2, Integer.valueOf(transactionKey));
						duplicateStatement.setInt(3, Integer.valueOf(from));
						duplicateStatement.setInt(4, Integer.valueOf(to));
						duplicateStatement.setTimestamp(5,
								getCurrentTimeStamp(time));
						duplicateStatement.setDouble(6, Double.valueOf(value));
						duplicateStatement.addBatch();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				//}
			}
		}
		//tmp2.clear();
	}
}
