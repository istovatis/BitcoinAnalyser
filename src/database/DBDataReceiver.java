package database;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Provide data from database.
 * 
 * @author istovatis
 *
 */
public class DBDataReceiver extends DBInteraction {

	protected int minTx;
	protected int maxTx;

	public int getMinTx() {
		return minTx;
	}

	public int getMaxTx() {
		return maxTx;
	}

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

	public static List<?> selectStringWhereClause(String field, String table,
			String where) {
		String select = "Select " + field + " From " + table + " Where "
				+ where;
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

	public static List<?> selectIntegerWhereClause(String field, String table,
			String where) {
		String select = "Select " + field + " From " + table + " Where "
				+ where;
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

	public static Integer count(String field, String table, boolean isDistinct) {
		String select = "SELECT count(" + (isDistinct ? "distinct" : "") + "("
				+ field + ")) " + " FROM " + table;
		int count = 0;
		try {
			preparedStatement = connection.prepareStatement(select);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				count = rs.getInt(1);
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
		return count;
	}

	public static List<?> selectQuery(String select) {
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

	public void findBounds(String table, String column) {
		System.out.println(java.lang.Runtime.getRuntime().maxMemory());
		connection = DBConnection.get().connectPostgre();
		String stats = "select min(" + column + "), max(" + column + ") from "
				+ table;
		try {
			preparedStatement = connection.prepareStatement(stats);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				minTx = rs.getInt(1);
				maxTx = rs.getInt(2);
				System.out.println();
				System.out.println("Scanning from " + minTx + " tx to " + maxTx
						+ " " + column + " from table " + table);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void readDataFile() {
		// TODO Auto-generated method stub

	}
}
