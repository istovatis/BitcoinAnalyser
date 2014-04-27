package bitcointools;
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

import database.DBConnection;
import database.DBInteraction;

import abe.Config;


public class UserKeyList extends DBInteraction {
	// public keys that belong to the same user
	private Integer[] keys;
	//private ArrayList<Integer> keyList;
	private HashSet<Integer[]> uniqueKeys = new HashSet<Integer[]>(); 
	
	private final String file = "userkey_list.txt";
	
	public UserKeyList(){
		table = "userkey_list";
	}
	public void readDataFile() {
		try {
			connection = DBConnection.get().connectPostgre();
			if(Config.isDBIntegration()){
				emptyTable();
			}
			br = new BufferedReader(new InputStreamReader(
					new DataInputStream(new FileInputStream(DBInteraction.path
							+ file))));
			String insertTableSQL = "INSERT INTO " + table
					+ "(key_number, public_keys) VALUES" + "(?,?)";
			preparedStatement = connection
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
				if(Config.isDBIntegration()){
					preparedStatement.setInt(1, rows + 1);
					preparedStatement.setArray(2, keys);
					preparedStatement.addBatch();
				}
				if(Config.isDublicatesIntegration()){
					if (!uniqueKeys.add(key)) {
						System.out.println("Double entry!" + (rows + 1) + ":"
								+ key.toString());
					}	
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
			System.out.println(rows + " Records added");
			System.out.println(rows - uniqueKeys.size() + " Duplicate keys");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			// System.err.println("Error: " + e.getMessage());
		}
	}

}
