package database;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Provide data from database.
 * @author istovatis 
 *
 */
public class DBDataReceiver extends DBInteraction {
	
	public static List<?> selectStringClause(String field, String table) {
		String select = "Select " + field + " From " + table;
		ArrayList<String> records = new ArrayList<String>();
		try {
			preparedStatement = connection.prepareStatement(select);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				records.add(rs.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getNextException());
		}
		
		finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		return records;
	}
	
	public static List<?> selectStringWhereClause(String field, String table, String where) {
		String select = "Select " + field + " From " + table + " Where "+ where;
		ArrayList<String> records = new ArrayList<String>();
		try {
			preparedStatement = connection.prepareStatement(select);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				records.add(rs.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getNextException());
		}
		
		finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		return records;
	}
	
	public static List<?> selectIntegerWhereClause(String field, String table, String where) {
		String select = "Select " + field + " From " + table + " Where "+ where;
		ArrayList<Integer> records = new ArrayList<Integer>();
		try {
			preparedStatement = connection.prepareStatement(select);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				records.add(rs.getInt(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getNextException());
		}
		
		finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		return records;
	}

	@Override
	public void readDataFile() {
		// TODO Auto-generated method stub
		
	}
}
