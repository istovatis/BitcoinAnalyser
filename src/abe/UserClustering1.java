package abe;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import bitcointools.HasParser;
import bitcointools.Database;

public class UserClustering1 extends HasParser {
	int minTx;
	int maxTx;
	ArrayList<HashSet<Integer>> groupedTxs;
	HashSet<Integer> addedTxs;
	ResultSet rs, rs2;
	Integer currentTx;
	Integer currentTxin;
	int[] ole = { 23, 13, 12, 45, 23, 45, 45, 13, 4, 5, 4, 4 };
	String entity = "entity";
	int numInsertedTxs;
	PreparedStatement updateStatement;

	public UserClustering1() {
		table = "entity";
	}

	public void clusterTxs() {
		try {
			groupedTxs = new ArrayList<HashSet<Integer>>();
			addedTxs = new HashSet<Integer>();
			Map<Integer, Integer> txToUser = new HashMap<Integer, Integer>();
			String sql = "SELECT tx_id, txin_id FROM txin WHERE txin_id = ? LIMIT 1";

			preparedStatement = connection.prepareStatement(sql);

			for (int i = minTx; i < maxTx / 10; i++) {
				preparedStatement.setInt(1, i);
				rs = preparedStatement.executeQuery();

				int selectedSet = -1;
				int ii = 0;
				while (rs.next()) {
					currentTx = rs.getInt(1); // ole[i];
					currentTxin = rs.getInt(2);
					if (txToUser.containsKey(currentTx))
						selectedSet = Integer.valueOf(txToUser.get(currentTx));
				}
				// rs.close();

				if (selectedSet != -1) {
					groupedTxs.get(selectedSet).add(currentTxin);

					txToUser.put(currentTx, selectedSet);

				} else {
					int size = groupedTxs.size();
					HashSet<Integer> tmp = new HashSet<Integer>();
					tmp.add(currentTxin);
					groupedTxs.add(tmp);
					txToUser.put(currentTx, size);
				}
				if (i % 500000 == 0)
					showInfo(i);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void clusterTxsFinal() {
		String insertTableSQL = "INSERT INTO " + entity + "(id, tx_ids) VALUES"
				+ "(?,?)";
		String updateTableSQL = "UPDATE " + entity
				+ " SET tx_ids = array_append(tx_ids, ?) where id = ?";
		Statement stSelect;
		try {
			stSelect = connection.createStatement();
			Statement stIndex = connection.createStatement();
			preparedStatement = connection.prepareStatement(insertTableSQL);
			updateStatement = connection.prepareStatement(updateTableSQL);
			System.out.println("Scanning from " + numInsertedTxs + " to "
					+ maxTx / 100);
			for (int i = numInsertedTxs; i <= maxTx; i++) {
				// String sql = "SELECT tx_id FROM txin WHERE txin_id = " +
				// i+" LIMIT 1";
				// rs = stSelect.executeQuery(sql);

				// while (rs.next()) {
				// currentTx = rs.getInt(1);
				String sqlIndex = "SELECT id FROM entity WHERE " + i
						+ "= any(tx_ids) LIMIT 1";
				rs = stIndex.executeQuery(sqlIndex);
				while (rs.next()) {
					int index = rs.getInt(1);
					// System.out.println(index+" "+currentTx);
					if (index < i) {
						preparedStatement.setInt(1, i);
						preparedStatement.setInt(2, i);
						;
						preparedStatement.executeUpdate();
					} else {
						updateStatement.setInt(1, i);
						Integer[] tmp = new Integer[] { i };
						Array sqlArray = connection.createArrayOf("VALUELIST",
								tmp);
						updateStatement.setArray(2, sqlArray);
						updateStatement.executeUpdate();
					}
				}
				// }
			}
			System.out.println("Inserted " + (maxTx - numInsertedTxs + 1)
					+ " entities successfully!");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void insertUsersToDB() {
		try {
			System.out.println("Insert users to Database... Starting at "
					+ new Date());
			String insertTableSQL = "INSERT INTO " + table
					+ "(id, tx_ids) VALUES" + "(?,?)";
			preparedStatement = connection.prepareStatement(insertTableSQL);

			for (int i = 0; i < groupedTxs.size(); i++) {
				preparedStatement.setInt(1, i + 1);
				// if(groupedTxs.get(i).size() > 4) {
				// System.out.print("Entity with txs "+groupedTxs.get(i).size());
				// for(Integer tx : groupedTxs.get(i))
				// System.out.print(tx+" ");
				// System.out.println();
				// }
				Integer[] tmp = groupedTxs.get(i).toArray(new Integer[0]);
				Array intArray = connection.createArrayOf("integer", tmp);
				preparedStatement.setArray(2, intArray);
				preparedStatement.addBatch();
				if (i % 100000 == 0) {
					addToDatabase(i, preparedStatement);
				}
			}
			preparedStatement.executeBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void cluster() {
		// String sql = "SELECT pubkey_id FROM txout WHERE tx_id = ?";
		String appendTx = "UPDATE " + table
				+ " SET pub_keys = array_append(pub_keys, ?) where tx_id = ?";
		try {
			// PreparedStatement stSelect = connection.prepareStatement(sql);
			Statement stSelect = connection.createStatement();
			preparedStatement = connection.prepareStatement(appendTx);
			for (int i = minTx; i < 5; i++) {
				String sql = "SELECT pubkey_id FROM txout WHERE tx_id = " + i;
				ResultSet rs = stSelect.executeQuery(sql);
				// stSelect.setInt(1, i);
				// ResultSet rs = stSelect.executeQuery(sql);
				while (rs.next()) {
					preparedStatement.setInt(1, i);
					preparedStatement.setInt(2, rs.getInt(1));
				}
				preparedStatement.addBatch();

				if (i % 4 == 0) {
					addToDatabase(i, preparedStatement);
				}
			}

			stSelect.close();
			preparedStatement.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void showInfo(int i) {
		int numEntities = groupedTxs.size();
		int groupedTxs = i - numEntities;
		System.out.println(i + " txins scanned at " + new Date());
		System.out.println("Entities: " + numEntities + ". Grouped entities: "
				+ groupedTxs);
	}

	@Override
	public void readDataFile() {
		// TODO Auto-generated method stub

	}

	public void findBounds(String table) {
		System.out.println(java.lang.Runtime.getRuntime().maxMemory());
		connection = Database.get().connectPostgre();
		String stats = "select min(txin_id), max(txin_id) from " + table;
		try {
			preparedStatement = connection.prepareStatement(stats);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				minTx = rs.getInt(1);
				maxTx = rs.getInt(2);
				System.out.println("Scanning from " + minTx + " tx to " + maxTx
						+ " tx.");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void findInsertedTxs() {
		connection = Database.get().connectPostgre();
		String stats = "select count(id) from " + entity;
		try {
			preparedStatement = connection.prepareStatement(stats);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				numInsertedTxs = rs.getInt(1);
				System.out.println(numInsertedTxs + " entities inserted into "
						+ entity + " table");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void start() {
		findBounds("txin");
		clusterTxs();
		insertUsersToDB();
		clusterTxsFinal();
	}
}
