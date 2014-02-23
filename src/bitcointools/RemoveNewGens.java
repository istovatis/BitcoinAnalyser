package bitcointools;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Date;
import java.util.Map;

import argo.jdom.JsonNode;
import argo.jdom.JsonStringNode;

public class RemoveNewGens extends HasParser {
	private final String file = "query results/new_generated";
	
	RemoveNewGens(){
		table = "user_edges_not_new_gen";
	}

	@Override
	public void readDataFile() {
		try {
			br = new BufferedReader(new InputStreamReader(new DataInputStream(
					new FileInputStream(HasParser.path + file))));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			String removeRecord = "DELETE FROM " + table
					+ " WHERE transaction_key = "
					+ "?";
			preparedStatement = connection.prepareStatement(removeRecord);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String strLine;
		int rows = 0;
		try {
			while ((strLine = br.readLine()) != null) {
				try {
					preparedStatement.setInt(1, Integer.valueOf(strLine));
					preparedStatement.addBatch();
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				rows++;
				if (rows % 1000 == 0) {
					Date date = new Date();
					System.out.println(rows +" rows proceeded "+date);
				}
			}
			try {
				preparedStatement.executeBatch();
				System.out.println(rows+ "Transactions detected and removed from table "+table);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
