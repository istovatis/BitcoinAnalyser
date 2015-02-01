package database;

import java.sql.SQLException;

/**
 * Use this Class to Update DB.
 *
 */
public class DBDataUpdater extends DBInteraction{
	
	
	public static void simpleUpdate(String query) {
		try {
			preparedStatement = connection.prepareStatement(query);
			preparedStatement.executeUpdate();
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
	
	public static void updateInteger(String table, String field, Integer value, String where) {
		String query = "UPDATE " + table
				+ " SET "+ field + " = ? Where " + where;
		try {
			preparedStatement = connection.prepareStatement(query);
			preparedStatement.setInt(1, value);
			preparedStatement.executeUpdate();
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
	
	public static void updateString(String table, String field, String value, String where) {
		String query = "UPDATE " + table
				+ " SET "+ field + " = ? WHERE " + where;
		try {
			preparedStatement = connection.prepareStatement(query);
			preparedStatement.setString(1, value);
			preparedStatement.executeUpdate();
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
