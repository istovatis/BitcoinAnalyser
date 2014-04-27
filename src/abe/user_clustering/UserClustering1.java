package abe.user_clustering;

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

import database.DBConnection;
import database.DBInteraction;
import database.Queries;

import abe.Filters;

public class UserClustering1 extends DBInteraction {
	int minTx;
	int maxTx;
	ArrayList<HashSet<Integer>> groupedTxs;
	HashSet<Integer> addedTxs;
	protected ResultSet rs, rs2;
	Integer currentTx, currentPubkey = 0;
	int[] ole = { 23, 13, 12, 45, 23, 45, 45, 13, 4, 5, 4, 4 };
	String entity = "entity";
	int numInsertedTxs;
	int entityId = 0;
	PreparedStatement updateStatement;
	final static int limit = 30;

	public UserClustering1() {
		table = "entity";
	}

	/**
	 * ( For all txin_ids , take tx_id and txin_id. Then, Create entities based
	 * on txins.
	 * 
	 * @throws SQLException
	 */
	public void clusterTxs() throws SQLException {
		try {
			groupedTxs = new ArrayList<HashSet<Integer>>();
			addedTxs = new HashSet<Integer>();
			Map<Integer, Integer> txToUser = new HashMap<Integer, Integer>();
			String sql = "SELECT tx_id FROM txin WHERE txin_id = ? LIMIT 1";

			preparedStatement = connection.prepareStatement(sql);

			for (int currentTxin = minTx; currentTxin < maxTx / limit; currentTxin++) {
				preparedStatement.setInt(1, currentTxin);
				rs = preparedStatement.executeQuery();

				int selectedSet = -1;
				while (rs.next()) {
					currentTx = rs.getInt(1); // ole[i];
					if (txToUser.containsKey(currentTx)) {
						selectedSet = Integer.valueOf(txToUser.get(currentTx));
						groupedTxs.get(selectedSet).add(currentTxin);
						txToUser.put(currentTx, selectedSet);
					} else {
						int size = groupedTxs.size();
						HashSet<Integer> tmp = new HashSet<Integer>();
						tmp.add(currentTxin);
						groupedTxs.add(tmp);
						txToUser.put(currentTx, size);
					}
				}
				// rs.close();

				if (currentTxin % 1000000 == 0)
					showInfo(currentTxin);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
	}

	/**
	 * Group all pubkeys that are txins at every tx
	 * 
	 * @return
	 * @throws SQLException
	 */
	public ArrayList<HashSet<Integer>> grouptTxsPubkeys() {
		try {
			System.out.println("Starting tx grouping of " + limit + "% at " + new Date());
			Map<Integer, Integer> pubKeyToTx = new HashMap<Integer, Integer>();
			groupedTxs = new ArrayList<HashSet<Integer>>();
			preparedStatement = connection.prepareStatement(Queries.pubkeysIdOfTxInputs());
			// pubkeys of txins that exist at the current tx
			HashSet<Integer> currentTxPuKeys = new HashSet<Integer>();
			for (int tx = minTx; tx < maxTx / limit; tx++) {
				preparedStatement.setInt(1, tx);
				rs = preparedStatement.executeQuery();
				boolean found = false;
				currentTxPuKeys = new HashSet<Integer>();
				int selectedSet = groupedTxs.size();
				while (rs.next()) {
					currentPubkey = rs.getInt(1);
					currentTxPuKeys.add(currentPubkey);
					// If pubkey already used, we want its tx
					if (pubKeyToTx.containsKey(currentPubkey)) {
						selectedSet = pubKeyToTx.get(currentPubkey);
						found = true;
					}
					// pubKeyToTx.put(currentPubkey, selectedSet);
					// } else if (currentPubkey != 0) {
					// int size = groupedTxs.size();
					// pubKeyToTx.put(currentPubkey, size == 0 ? size : size -
					// 1);
					// }
				}
				if (!currentTxPuKeys.contains(0)) {
					for (Integer pubkey : currentTxPuKeys) {
						pubKeyToTx.put(pubkey, selectedSet);
					}
					if (found) {
						groupedTxs.get(selectedSet).addAll(currentTxPuKeys);
					} else {
						groupedTxs.add(currentTxPuKeys);
					}
				}
				// rs.close();
				if (tx % 1000000 == 0)
					showInfo(tx);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("That's it: " + groupedTxs.size());
		return groupedTxs;
	}

	public void clusterTxsFinal() throws SQLException {
		String insertTableSQL = "INSERT INTO " + entity + "(id, tx_ids) VALUES" + "(?,?)";
		String updateTableSQL = "UPDATE " + entity + " SET tx_ids = array_append(tx_ids, ?) where id = ?";
		Statement stSelect;
		stSelect = connection.createStatement();
		Statement stIndex = connection.createStatement();
		preparedStatement = connection.prepareStatement(insertTableSQL);
		updateStatement = connection.prepareStatement(updateTableSQL);
		System.out.println("clusterTxsFinal: Scanning from " + numInsertedTxs + " to " + maxTx / limit);
		for (int i = numInsertedTxs; i <= maxTx; i++) {
			String sqlIndex = "SELECT id FROM entity WHERE " + i + "= any(tx_ids) LIMIT 1";
			rs = stIndex.executeQuery(sqlIndex);
			while (rs.next()) {
				int index = rs.getInt(1);
				// System.out.println(index+" "+currentTx);
				if (index < i) {
					preparedStatement.setInt(1, i);
					preparedStatement.setInt(2, i);
					preparedStatement.executeUpdate();
				} else {
					updateStatement.setInt(1, i);
					Integer[] tmp = new Integer[] { i };
					Array sqlArray = connection.createArrayOf("VALUELIST", tmp);
					updateStatement.setArray(2, sqlArray);
					updateStatement.executeUpdate();
				}
			}
		}
		System.out.println("Inserted " + (maxTx - numInsertedTxs + 1) + " entities successfully!");
	}

	public void createEntityTable() throws SQLException {
		System.out.println("Creating Entity table... Starting at " + new Date());
		String insertTableSQL = "INSERT INTO " + table + "(id, pub_keys) VALUES" + "(?,?)";
		PreparedStatement insertStatement = connection.prepareStatement(insertTableSQL);
		HashSet<Integer> pubKey = new HashSet<Integer>();
		int tmp = 0;
		for (int i = 0; i < groupedTxs.size(); i++) {
			pubKey.clear();
			int size = groupedTxs.get(i).size();
			Integer[] dbArr = (Integer[]) groupedTxs.get(i).toArray(new Integer[size]);
			Array pubKeys = connection.createArrayOf("integer", dbArr);
			if (size == 1)
				tmp++;
			insertStatement.setInt(1, i + 1);

			insertStatement.setArray(2, pubKeys);

			insertStatement.addBatch();
			if (i % 200000 == 0) {
				addToDatabase(i, insertStatement);
			}
		}
		System.out.println(tmp);
		insertStatement.executeBatch();

	}

	public void createEntityIndexTable() throws SQLException {
		System.out.println("Creating Entity index table... Starting at " + new Date());
		String insertTableSQL = "INSERT INTO " + "entity_index" + " (entity_id, pub_key) VALUES " + "(?,?) ";
		PreparedStatement insertStatement = connection.prepareStatement(insertTableSQL);
		HashSet<Integer> pubKeys;
		for (int i = 0; i < groupedTxs.size(); i++) {
			pubKeys = new HashSet<Integer>();
			for (Integer pubKey : groupedTxs.get(i)) {
				if (pubKey == 186 || pubKey == 10)
					System.out.println(pubKey + "," + i);
				insertStatement.setInt(1, i + 1);
				insertStatement.setInt(2, pubKey);
				insertStatement.addBatch();
				if (i % 200000 == 0) {
					addToDatabase(i, insertStatement);
				}
			}
		}
		insertStatement.executeBatch();
	}

	/**
	 * d
	 * 
	 * @throws SQLException
	 */
	public void createUserEdgesTable() throws SQLException {
		System.out.println("Creating User Edge table... Starting at " + new Date());
		String sqlFindFromPubKey = "Select entity_index.entity_id from txout inner join txin on txout.txout_id = txin.txout_id  inner join entity_index on txout.pubkey_id = entity_index.pub_key where txin.tx_id = ? and txin.txout_id != 0 LIMIT 1 ";
		String sqlFindToPubKey = "Select entity_index.entity_id from txout inner join entity_index on txout.pubkey_id = entity_index.pub_key where txout.tx_id = ? ";
		String insertToUserEdges = "Insert into user_edge (id, from_user, to_user) values  (?, ?, ?)";
		PreparedStatement fromStatement = connection.prepareStatement(sqlFindFromPubKey);
		updateStatement = connection.prepareStatement(sqlFindToPubKey);
		PreparedStatement insertStatement = connection.prepareStatement(insertToUserEdges);

		// findBounds("tx", "tx_id");
		Filters filter = new Filters();
		HashSet<Integer> notNewGens = filter.eliminateCoinGens(maxTx / limit);
		int i = 0;
		for (Integer tx : notNewGens) {
			int from = 0;
			ArrayList<Integer> to = new ArrayList<Integer>();
			fromStatement.setInt(1, tx);
			updateStatement.setInt(1, tx);
			rs = fromStatement.executeQuery();
			while (rs.next()) {
				from = rs.getInt(1);
			}

			rs = updateStatement.executeQuery();
			while (rs.next()) {
				to.add(rs.getInt(1));
			}

			if (from != 0) {
				for (int k = 0; k < to.size(); k++) {
					insertStatement.setInt(1, tx);
					insertStatement.setInt(2, from);
					insertStatement.setInt(3, to.get(k));
					insertStatement.addBatch();
				}
				if (i % (batchSize / 100) == 0) {
					addToDatabase(i, insertStatement);
				}
			}
			i++;
		}
		insertStatement.executeBatch();
	}

	public void createUserEdgesTable2() throws SQLException {
		System.out.println("Creating User Edge table... Max tx:" + maxTx / limit + " Starting at " + new Date());
		HashSet<Integer> txs = new HashSet<Integer>();
		String sqlFindFromPubKey = "Select entity_index.entity_id, txin.tx_id from txout inner join txin on txout.txout_id = txin.txout_id  inner join entity_index on txout.pubkey_id = entity_index.pub_key where txin.tx_id <= ? and txin.txout_id != 0 ";
		String sqlFindToPubKey = "Select entity_index.entity_id, txout.tx_id from txout inner join entity_index on txout.pubkey_id = entity_index.pub_key where txout.tx_id <= ? ";
		String insertFromUserEdges = "Insert into user_edge (id, from_user) values  (?, ?) ";
		String insertToUserEdges = "Update user_edge set to_user = ? where id = ? ";

		PreparedStatement fromStatement = connection.prepareStatement(sqlFindFromPubKey);
		PreparedStatement toStatement = connection.prepareStatement(sqlFindToPubKey);
		PreparedStatement insertFromStatement = connection.prepareStatement(insertFromUserEdges);
		PreparedStatement insertToStatement = connection.prepareStatement(insertToUserEdges);

		int from = 0, to = 0, tx = 0;
		fromStatement.setInt(1, maxTx / limit);
		toStatement.setInt(1, maxTx / limit);

		rs = fromStatement.executeQuery();
		while (rs.next()) {
			tx = rs.getInt(2);
			if (txs.add(tx)) {
				from = rs.getInt(1);
				insertFromStatement.setInt(1, tx);
				insertFromStatement.setInt(2, from);
				insertFromStatement.addBatch();
			}
		}

		rs = toStatement.executeQuery();
		while (rs.next()) {
			to = rs.getInt(1);
			tx = rs.getInt(2);
			insertToStatement.setInt(1, to);
			insertToStatement.setInt(2, tx);
			insertToStatement.addBatch();
		}

		insertFromStatement.executeBatch();
		insertToStatement.executeBatch();
	}

	/**
	 * Like createUserEdgesTable3 but inserts user edge data in a different way 
	 * @throws SQLException
	 */
	public void createUserEdgesTable3() throws SQLException {
		System.out.println("Creating User Edge table... Max tx:" + maxTx / limit + " Starting at " + new Date());
		
		String sqlFindFromPubKey = "Select entity_index.entity_id, txin.tx_id from txout inner join txin on txout.txout_id = txin.txout_id  inner join entity_index on txout.pubkey_id = entity_index.pub_key where txin.tx_id <= ? and txin.txout_id != 0 ";
		String sqlFindToPubKey = "Select entity_index.entity_id, txout.tx_id from txout inner join entity_index on txout.pubkey_id = entity_index.pub_key where txout.tx_id <= ? ";
		String insertToUserEdges = "INSERT INTO user_edge (to_user, id) values (?, ?) ";
		String insertFromUserEdges = "UPDATE user_edge  set from_user = ? where id = ? ";
		
		PreparedStatement fromStatement = connection.prepareStatement(sqlFindFromPubKey);
		PreparedStatement toStatement = connection.prepareStatement(sqlFindToPubKey);
		PreparedStatement insertFromStatement = connection.prepareStatement(insertFromUserEdges);
		PreparedStatement insertToStatement = connection.prepareStatement(insertToUserEdges);

		int from = 0, to = 0, tx = 0;
		fromStatement.setInt(1, maxTx / limit);
		toStatement.setInt(1, maxTx / limit);

		rs = toStatement.executeQuery();
		while (rs.next()) {
			to = rs.getInt(1);
			tx = rs.getInt(2);
			insertToStatement.setInt(1, to);
			insertToStatement.setInt(2, tx);
			insertToStatement.addBatch();
		}
		
		rs = fromStatement.executeQuery();
		while (rs.next()) {
			tx = rs.getInt(2);
			from = rs.getInt(1);
			insertFromStatement.setInt(1, from);
			insertFromStatement.setInt(2, tx);
			insertFromStatement.addBatch();		
		}

		insertToStatement.executeBatch();
		insertFromStatement.executeBatch();
	}

	public void cluster() {
		// String sql = "SELECT pubkey_id FROM txout WHERE tx_id = ?";
		String appendTx = "UPDATE " + table + " SET pub_keys = array_append(pub_keys, ?) where tx_id = ?";
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
		System.out.println("Entities: " + numEntities + ". Grouped entities: " + groupedTxs);
	}

	@Override
	public void readDataFile() {
		// TODO Auto-generated method stub

	}

	public void findBounds(String table, String column) throws SQLException {
		System.out.println(java.lang.Runtime.getRuntime().maxMemory());
		connection = DBConnection.get().connectPostgre();
		String stats = "select min(" + column + "), max(" + column + ") from " + table;
		preparedStatement = connection.prepareStatement(stats);
		ResultSet rs = preparedStatement.executeQuery();
		while (rs.next()) {
			minTx = rs.getInt(1);
			maxTx = rs.getInt(2);
			System.out.println("Scanning from " + minTx + " tx to " + maxTx / limit + " " + column + " from table "
					+ table);
		}
	}

	public void findInsertedTxs() throws SQLException {
		connection = DBConnection.get().connectPostgre();
		String stats = "select count(id) from " + entity;
		preparedStatement = connection.prepareStatement(stats);
		ResultSet rs = preparedStatement.executeQuery();
		while (rs.next()) {
			numInsertedTxs = rs.getInt(1);
			System.out.println(numInsertedTxs + " entities inserted into " + entity + " table");
		}
	}

	public void start() {
		try {
			findBounds("txin", "txin_id");
			// grouptTxsPubkeys();
			// createEntityTable();
			// createEntityIndexTable();
			createUserEdgesTable3();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getNextException());
		}
	}
}
