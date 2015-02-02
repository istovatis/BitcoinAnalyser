package bitcointools;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

import database.DBInteraction;
import database.Queries;

import abe.Config;


/**
 * This class keeps all transaction data
 * 
 * @author istovatis
 * 
 */
public class UserEdges extends DBInteraction {
	protected int transactionKey;
	private int from;
	private int to;
	private int date;
	private Timestamp time;
	private double value;
	private int line; // line number at parsed file

	public UserEdges() {
		table = "user_edges_not_new_gen";
		duplicateTable = "user_edges_duplicates";
	}

	private final String file = "user_edges.txt";
	SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
	SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("hh:mm:ss");
	HashSet<ArrayList<Number>> uniqueKey = new HashSet<ArrayList<Number>>();

	public void readDataFile() {
		try {
			br = new BufferedReader(new InputStreamReader(new DataInputStream(
					new FileInputStream(DBInteraction.path + file))));
			String insertTableSQL = "INSERT INTO "
					+ table
					+ "(line, transaction_key, user_from_key, user_key_to, date, value) VALUES"
					+ "(?,?,?,?,?,?)";
			preparedStatement = connection.prepareStatement(insertTableSQL);

			if (Config.isDublicatesIntegration()) {
				String insertDublicateSQL = "INSERT INTO "
						+ duplicateTable
						+ "(line, transaction_key, user_from_key, user_key_to, date, value) VALUES"
						+ "(?,?,?,?,?,?)";
				duplicateStatement = connection
						.prepareStatement(insertDublicateSQL);
			}
			String strLine;
			int rows = 0;
			if (Config.isDBIntegration()) {
				System.out.println(emptyTable() + " Records deleted from "
						+ table);
			}
			if (Config.isDublicatesIntegration()) {
				System.out.println(emptyTable(this.duplicateTable)
						+ " Records deleted from " + this.duplicateTable);
			}
			while ((strLine = br.readLine()) != null) {
				String[] tags = strLine.split(",");
				line = rows + 1;
				transactionKey = Integer.valueOf(tags[0]);
				from = Integer.valueOf(tags[1]);
				to = Integer.valueOf(tags[2]);
				value = Double.valueOf(tags[4]);
				time = getCurrentTimeStamp(returnDate(tags[3]));
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				Date date1 = sdf.parse("2009-01-10");
				if (time.before(date1)) {
					System.out.println("Double entry! " + line + " "
							+ transactionKey + "  " + from + "  " + to + "  "
							+ value + "  " + time);
				}
				preparedStatement.setInt(1, Integer.valueOf(line));
				preparedStatement.setInt(2, Integer.valueOf(tags[0]));
				preparedStatement.setInt(3, Integer.valueOf(tags[1]));
				preparedStatement.setInt(4, Integer.valueOf(tags[2]));
				preparedStatement.setTimestamp(5,
						getCurrentTimeStamp(returnDate(tags[3])));
				preparedStatement.setDouble(6, Double.valueOf(tags[4]));
			
				if (Config.isDBIntegration()) {
					// execute insert SQL stetement
					preparedStatement.addBatch();
				}
				if (Config.isDublicatesIntegration()) {
					checkForPublicates();
				}
				if (rows % batchSize == 0) {
					System.out.println("Records:" + (rows + 1) + " at "
							+ new Date());
					if (Config.isDBIntegration()) {
						addToDatabase(rows, preparedStatement);
						System.out.println((rows + 1) - uniqueKey.size()
								+ " Duplicate keys");
					}
					if (Config.isDublicatesIntegration()) {
						addToDatabase(rows, duplicateStatement);
					}
				}
				rows++;
			}
			preparedStatement.executeBatch();
			if (Config.isDublicatesIntegration()) {
				duplicateStatement.executeBatch();
			}
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
			if (Config.isDublicatesIntegration()) {
				// if(dublicateKey.add(tmp2)){
				try {
					duplicateStatement.setInt(1, Integer.valueOf(line));
					duplicateStatement.setInt(2,
							Integer.valueOf(transactionKey));
					duplicateStatement.setInt(3, Integer.valueOf(from));
					duplicateStatement.setInt(4, Integer.valueOf(to));
					duplicateStatement.setTimestamp(5,
							getCurrentTimeStamp(time));
					duplicateStatement.setDouble(6, Double.valueOf(value));
					duplicateStatement.addBatch();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Get all tx ids and search for their hashes. Write tx_id;hash to file
	 */
	public void matchTxsWithHashes() {
		String outF = File.separator + "query_results" + File.separator
				+ "id_matching_hash3";
		try {
			Date date = new Date();
			System.out.println("Matching  txs starting with Hashes " + date);
			File outFile = new File(path + outF);
			// if file doesnt exists, then create it
			if (!outFile.exists()) {
				outFile.createNewFile();
			}
			FileWriter fw = new FileWriter(outFile.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			br = new BufferedReader(new InputStreamReader(new DataInputStream(
					new FileInputStream(DBInteraction.path + file))));
			int count = 0;
			// Read just the first line and do nothing
			String strLine = br.readLine();
			while ((strLine = br.readLine()) != null) {
				String[] splits = strLine.split(",");
				Integer id = Integer.valueOf(splits[0]);
				if (Transaction.txIds.add(id)) {
					try {
						preparedStatement = connection.prepareStatement(Queries
								.getTxHashString(id));
						ResultSet rs = preparedStatement.executeQuery();
						while (rs.next()) {
							String hash = rs.getString(1);
							String content = id + ";" + hash;
							bw.write(content);
							bw.newLine();
						}
					} catch (SQLException e) {
						e.printStackTrace();
					}

				}
				count++;
				if (count % 1000000 == 0) {
					System.out.println(count + " records parsed.");
				}
			}
			bw.close();
			Date date2 = new Date();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void addNewLine() {
		String outF = File.separator + "query_results" + File.separator
				+ "id_matching_hash4";
		String inF = File.separator + "query_results" + File.separator
				+ "id_matching_hash3";
		try {
			Date date = new Date();
			System.out.println("Matching  txs starting with Hashes " + date);
			File outFile = new File(path + outF);
			// if file doesnt exists, then create it
			if (!outFile.exists()) {
				outFile.createNewFile();
			}
			FileWriter fw = new FileWriter(outFile.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			br = new BufferedReader(new InputStreamReader(new DataInputStream(
					new FileInputStream(DBInteraction.path + inF))));
			int count = 0;
			// Read just the first line and do nothing
			String strLine;
			while ((strLine = br.readLine()) != null) {

				String[] splits = strLine.split(";");
				Integer id = Integer.valueOf(splits[0]);
				if (Transaction.txIds.add(id)) {
					bw.write(strLine);
					bw.newLine();
				}
				count++;
				if (count % 1000000 == 0) {
					Date d = new Date();
					System.out.println(count + " records parsed at " + d);
				}
			}
			bw.close();
			Date date2 = new Date();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Seperate a single file into many
	 */
	public void seperateFiles() {

		int records = 15898622; // total number of records
		int numFiles = 16;
		int range = records / numFiles;
		System.out.println(range);
		String files[] = new String[numFiles];
		File outFiles[] = new File[numFiles];
		FileWriter fws[] = new FileWriter[numFiles];
		BufferedWriter[] bw = new BufferedWriter[numFiles];
		String inPath = File.separator + "query_results" + File.separator;
		String inF = File.separator + "query_results" + File.separator
				+ "id_matching_hash4";

		for (int i = 0; i < numFiles; i++) {
			files[i] = "input" + i;
			outFiles[i] = new File(path + inPath + files[i]);

			try {
				if (!outFiles[i].exists()) {
					outFiles[i].createNewFile();
				}
				fws[i] = new FileWriter(outFiles[i].getAbsoluteFile());
			} catch (IOException e) {
				e.printStackTrace();
			}
			bw[i] = new BufferedWriter(fws[i]);
		}

		try {
			Date date = new Date();
			System.out.println("Splitting file starting at " + date);

			br = new BufferedReader(new InputStreamReader(new DataInputStream(
					new FileInputStream(DBInteraction.path + inF))));
			int count = 0;
			// Read just the first line and do nothing
			String strLine;
			while ((strLine = br.readLine()) != null) {
				int selectedFile = count / range;
				if (selectedFile > 15) {
					selectedFile = 15;
				}
				bw[selectedFile].write(strLine);
				bw[selectedFile].newLine();
				count++;
				if (count % 1000000 == 0) {
					Date d = new Date();
					System.out.println(count + " records parsed at " + d);
				}
			}

			for (int i = 0; i < numFiles; i++) {
				bw[i].close();
			}
			Date date2 = new Date();
			System.out.println(count);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void countSplitFilesLines(){
		int records = 15898622; // total number of records
		int numFiles = 16;
		int range = records / numFiles;
		System.out.println(range);
		int globalCounter = 0;

		String files[] = new String[numFiles];
		String inPath = File.separator + "query_results" + File.separator;
		int counter = 0;
		for(int i=0; i<numFiles;i++){
			files[i] = path+inPath+"input"+i;
			try {
				br = new BufferedReader(new InputStreamReader(new DataInputStream(
						new FileInputStream(files[i]))));
				String strLine;
				try {
					while ((strLine = br.readLine()) != null) {
						counter++;
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			System.out.println("Parsed file "+i+ " with "+counter+" records");
			globalCounter+= counter;
			counter = 0;
		}
		System.out.println("total records: "+globalCounter);
	}
}
