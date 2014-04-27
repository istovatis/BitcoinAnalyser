package database;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import clasiffier.mining_pool.MiningPool;

public class DBDataProvider extends DBInteraction {
	
	public void insertStringClause(String field, String table, ArrayList<String> records) {
		String insertTableSQL = "INSERT INTO "+ table
				+ "(" + field +") VALUES (?)";
		try {
			preparedStatement = connection
					.prepareStatement(insertTableSQL);
			for (String record : records) {
				preparedStatement.setString(1, record);
				preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getNextException());
		} finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void insertIntegerClause(String table, String field, ArrayList<Integer> records) {
		String insertTableSQL = "INSERT INTO "+ table
				+ "(" + field +") VALUES (?)";
		try {
			preparedStatement = connection
					.prepareStatement(insertTableSQL);
			for (Integer record : records) {
				preparedStatement.setInt(1, record);
				preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getNextException());
		} finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void insertIntegerClauseWhere(String table, Integer value,String field, String where) {
		String insertTableSQL = "INSERT INTO "+ table
				+ "(" + field +") VALUES (?) WHERE "+where;
		try {
			preparedStatement = connection
					.prepareStatement(insertTableSQL);
			preparedStatement.setInt(1, value);
			preparedStatement.addBatch();
			preparedStatement.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getNextException());
		} finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	@Override
	public void readDataFile() {
		// TODO Auto-generated method stub
		
	}

}
