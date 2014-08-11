package abe.user_clustering;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

import abe.Config;
import database.DBInteraction;

public class UserEdge extends DBInteraction {
	
	private final String file = "fix_user_edge2";
	private int line;
	private Integer tx_id;
	private Integer entity_id;
	
	public UserEdge() {
		table = "user_edge";
		batchSize = 1000;
	}
	@Override
	public void readDataFile() {
		try {
			br = new BufferedReader(new InputStreamReader(new DataInputStream(
					new FileInputStream(DBInteraction.path + file))));
			String updateSQL = "UPDATE " + table + " set from_user = ? where id = ? ";
			preparedStatement = connection.prepareStatement(updateSQL);
			String strLine;
			int rows = 0;
			
			while ((strLine = br.readLine()) != null) {
				String[] tags = strLine.split(";");
				line = rows + 1;
				tx_id = Integer.valueOf(tags[0]);
				entity_id = Integer.valueOf(tags[1]);
				
				preparedStatement.setInt(1, entity_id);
				preparedStatement.setInt(2, tx_id);
				preparedStatement.addBatch();
						
				if (rows % batchSize == 0) {
					System.out.println("Records:" + (rows + 1) + " at "
							+ new Date());
				addToDatabase(rows, preparedStatement);
				}
				rows++;
			}
			preparedStatement.executeBatch();
			System.out.println(rows + " Records fixed from user edge. Houraaaa!!!!!");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		//update user_edge2 set from_user = 19177169 where id = 7629569
	}
	
}
