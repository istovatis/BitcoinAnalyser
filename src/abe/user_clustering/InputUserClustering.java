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
import java.util.Map;

import database.DBConnection;
import database.DBDataReceiver;
import database.DBDataUpdater;
import database.DBInteraction;
import database.Queries;
import abe.Filters;

public class InputUserClustering extends DBInteraction {
	int minTx;
	int maxTx;
	private Map<Integer, Integer> pubKeyToTx;
	ArrayList<HashSet<Integer>> groupedTxs;
	HashSet<Integer> addedTxs;
	protected ResultSet rs, rs2;
	Integer currentTx, currentPubkey = 0;
	int[] ole = { 23, 13, 12, 45, 23, 45, 45, 13, 4, 5, 4, 4 };
	String entity = "entity";
	int numInsertedTxs;
	int entityId = 0;
	PreparedStatement updateStatement;
	final static int limit = 1;

	public InputUserClustering() {
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
					currentTx = rs.getInt(1);
					if (txToUser.containsKey(currentTx)) {
						selectedSet = Integer.valueOf(txToUser.get(currentTx));
						groupedTxs.get(selectedSet).add(currentTxin);
						txToUser.put(currentTx, selectedSet);
					} else {
						HashSet<Integer> tmp = new HashSet<Integer>();
						tmp.add(currentTxin);
						groupedTxs.add(tmp);
						txToUser.put(currentTx, groupedTxs.size());
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
	public ArrayList<HashSet<Integer>> groupTxsPubkeys() {
		try {
			System.out.println("Starting tx grouping of " + limit + "% at "
					+ new Date());
			Map<Integer, Integer> pubKeyToTx = new HashMap<Integer, Integer>();
			groupedTxs = new ArrayList<HashSet<Integer>>();
			preparedStatement = connection.prepareStatement(Queries
					.pubkeysIdOfTxInputs());
			// pubkeys of txins that exist at the current tx
			for (int tx = minTx; tx < maxTx / limit; tx++) {
				preparedStatement.setInt(1, tx);
				rs = preparedStatement.executeQuery();
				boolean found = false;
				HashSet<Integer> currentTxPuKeys = new HashSet<Integer>();
				int selectedSet = groupedTxs.size();
				while (rs.next()) {
					currentPubkey = rs.getInt(1);
					currentTxPuKeys.add(currentPubkey);
					// If pubkey already used, we want its tx
					if (pubKeyToTx.containsKey(currentPubkey)) {
						selectedSet = pubKeyToTx.get(currentPubkey);
						found = true;
					}
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
		System.out
				.println("That's it. Clustering using inputs has been completed. Created : "
						+ groupedTxs.size() + " entities");
		return groupedTxs;
	}

	public ArrayList<HashSet<Integer>> lightGroupTxsPubkeys() {
		try {
			System.out.println("Starting tx grouping of " + limit + "% at "
					+ new Date());
			int numEntities = 0;
			pubKeyToTx = new HashMap<Integer, Integer>();
			preparedStatement = connection.prepareStatement(Queries
					.pubkeysIdOfTxInputs());
			// pubkeys of txins that exist at the current tx
			for (int tx = minTx; tx < maxTx / limit; tx++) {
				preparedStatement.setInt(1, tx);
				rs = preparedStatement.executeQuery();
				boolean found = false;
				HashSet<Integer> currentTxPuKeys = new HashSet<Integer>();
				int selectedSet = numEntities;
				while (rs.next()) {
					currentPubkey = rs.getInt(1);
					currentTxPuKeys.add(currentPubkey);
					// If pubkey already used, we want its tx
					if (pubKeyToTx.containsKey(currentPubkey)) {
						selectedSet = pubKeyToTx.get(currentPubkey);
						found = true;
					}
				}
				if (!currentTxPuKeys.contains(0)) {
					for (Integer pubkey : currentTxPuKeys) {
						pubKeyToTx.put(pubkey, selectedSet);
					}
					if (!found)
						numEntities++;
				}
				if (tx % 1000000 == 0)
					System.out.println(tx + " transactions scanned at "
							+ new Date());
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
		System.out
				.println("That's it. Clustering using inputs has been completed. ");
		return groupedTxs;
	}

	public void clusterTxsFinal() throws SQLException {
		String insertTableSQL = "INSERT INTO " + entity + "(id, tx_ids) VALUES"
				+ "(?,?)";
		String updateTableSQL = "UPDATE " + entity
				+ " SET tx_ids = array_append(tx_ids, ?) where id = ?";
		Statement stSelect;
		stSelect = connection.createStatement();
		Statement stIndex = connection.createStatement();
		preparedStatement = connection.prepareStatement(insertTableSQL);
		updateStatement = connection.prepareStatement(updateTableSQL);
		System.out.println("clusterTxsFinal: Scanning from " + numInsertedTxs
				+ " to " + maxTx / limit);
		for (int i = numInsertedTxs; i <= maxTx; i++) {
			String sqlIndex = "SELECT id FROM entity WHERE " + i
					+ "= any(tx_ids) LIMIT 1";
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
		System.out.println("Inserted " + (maxTx - numInsertedTxs + 1)
				+ " entities successfully!");
	}

	public void createEntityTable() throws SQLException {
		System.out
				.println("Creating Entity table... Starting at " + new Date());
		String insertTableSQL = "INSERT INTO " + table
				+ "(id, pub_keys) VALUES" + "(?,?)";
		PreparedStatement insertStatement = connection
				.prepareStatement(insertTableSQL);
		HashSet<Integer> pubKey = new HashSet<Integer>();
		int tmp = 0;
		for (int i = 0; i < groupedTxs.size(); i++) {
			pubKey.clear();
			int size = groupedTxs.get(i).size();
			Integer[] dbArr = (Integer[]) groupedTxs.get(i).toArray(
					new Integer[size]);
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
		System.out.println("Creating Entity index table... Starting at "
				+ new Date());
		String insertTableSQL = "INSERT INTO " + "entity_index"
				+ " (entity_id, pub_key) VALUES " + "(?,?) ";
		PreparedStatement insertStatement = connection
				.prepareStatement(insertTableSQL);
		HashSet<Integer> pubKeys;
		for (int i = 0; i < groupedTxs.size(); i++) {
			pubKeys = new HashSet<Integer>();
			for (Integer pubKey : groupedTxs.get(i)) {
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

	public void lightCreateEntityIndexTable() throws SQLException {
		System.out.println("Creating light Entity index table... Starting at "
				+ new Date());
		String insertTableSQL = "INSERT INTO " + "light_entity_index"
				+ " (entity_id, pub_key) VALUES " + "(?,?) ";
		PreparedStatement insertStatement = connection
				.prepareStatement(insertTableSQL);
		int id = 0;
		for (Map.Entry<Integer, Integer> entry : pubKeyToTx.entrySet()) {
			Integer key = entry.getKey();
			Integer value = entry.getValue();
			insertStatement.setInt(1, value);
			insertStatement.setInt(2, key);
			insertStatement.addBatch();
			if (id % 200000 == 0) {
				addToDatabase(id, insertStatement);
			}
		}
		insertStatement.executeBatch();
	}

	/**
	 * Insert pubkey that exist only as outputs at entity index table. This
	 * method should be used after inputUserClustering.
	 */
	public void insertOnlyInputsAtEntityIndex() {
		System.out
				.println("Find and insert pub keys that exist only as outputs and are not advance shadows at "
						+ new Date());
		String insertTableSQL = "INSERT INTO light_entity_index (entity_id, pub_key, only_out) VALUES"
				+ "(?, ?, true)";
		String selectOutsSQL = "SELECT a.pubkey_id  FROM pubkey a LEFT JOIN light_entity_index b ON a.pubkey_id = b.pub_key LEFT JOIN advance_shadow c ON a.pubkey_id = c.pubkey_id WHERE b.pub_key IS NULL and c.pubkey_id IS NULL";
		int id = DBDataReceiver.count("entity_id", "light_entity_index", false);
		System.out.println("first given id :" + id);
		try {
			Statement stSelect = connection.createStatement();
			PreparedStatement insertStatement = connection
					.prepareStatement(insertTableSQL);
			rs = stSelect.executeQuery(selectOutsSQL);
			while (rs.next()) {
				int pubkey = rs.getInt(1);
				insertStatement.setInt(1, ++id);
				insertStatement.setInt(2, pubkey);
				insertStatement.addBatch();
				if (id % 200000 == 0) {
					addToDatabase(id, insertStatement);
				}
			}
			insertStatement.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out
				.println("Insertion of pubkeys that exist only as outputs ended at "
						+ new Date());

	}

	/**
	 * Insert advance shadows at light entity index. This method should be used
	 * after inputUserClustering.
	 */
	public void insertAdvanceShadowAtEntityIndex() {
		System.out.println("Find and insert  advance shadows at " + new Date());
		String insertTableSQL = "INSERT INTO light_entity_index (entity_id, pub_key, only_out, advance_shadow) VALUES"
				+ "(?, ?, true, true)";
		String selectOutsSQL = "SELECT pubkey_id FROM  advance_shadow";
		int id = DBDataReceiver.count("entity_id", "light_entity_index", false);
		System.out.println("first given id :" + id);

		try {
			Statement stSelect = connection.createStatement();
			PreparedStatement insertStatement = connection
					.prepareStatement(insertTableSQL);
			rs = stSelect.executeQuery(selectOutsSQL);
			while (rs.next()) {
				int pubkey = rs.getInt(1);
				insertStatement.setInt(1, ++id);
				insertStatement.setInt(2, pubkey);
				insertStatement.addBatch();
				if (id % 200000 == 0) {
					addToDatabase(id, insertStatement);
				}
			}
			insertStatement.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out
				.println("Insertion of advance shadow ended at " + new Date());
	}

	/**
	 * d
	 * 
	 * @throws SQLException
	 */
	public void createUserEdgesTable() throws SQLException {
		System.out.println("Creating User Edge table... Starting at "
				+ new Date());
		String sqlFindFromPubKey = "Select entity_index.entity_id from txout inner join txin on txout.txout_id = txin.txout_id  inner join entity_index on txout.pubkey_id = entity_index.pub_key where txin.tx_id = ? and txin.txout_id != 0 LIMIT 1 ";
		String sqlFindToPubKey = "Select entity_index.entity_id from txout inner join entity_index on txout.pubkey_id = entity_index.pub_key where txout.tx_id = ? ";
		String insertToUserEdges = "Insert into user_edge (id, from_user, to_user) values  (?, ?, ?)";
		PreparedStatement fromStatement = connection
				.prepareStatement(sqlFindFromPubKey);
		updateStatement = connection.prepareStatement(sqlFindToPubKey);
		PreparedStatement insertStatement = connection
				.prepareStatement(insertToUserEdges);

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
		System.out.println("Creating User Edge table... Max tx:" + maxTx
				/ limit + " Starting at " + new Date());
		HashSet<Integer> txs = new HashSet<Integer>();
		String sqlFindFromPubKey = "Select entity_index.entity_id, txin.tx_id from txout inner join txin on txout.txout_id = txin.txout_id  inner join entity_index on txout.pubkey_id = entity_index.pub_key where txin.tx_id <= ? and txin.txout_id != 0 ";
		String sqlFindToPubKey = "Select entity_index.entity_id, txout.tx_id from txout inner join entity_index on txout.pubkey_id = entity_index.pub_key where txout.tx_id <= ? ";
		String insertFromUserEdges = "Insert into user_edge (id, from_user) values  (?, ?) ";
		String insertToUserEdges = "Update user_edge set to_user = ? where id = ? ";

		PreparedStatement fromStatement = connection
				.prepareStatement(sqlFindFromPubKey);
		PreparedStatement toStatement = connection
				.prepareStatement(sqlFindToPubKey);
		PreparedStatement insertFromStatement = connection
				.prepareStatement(insertFromUserEdges);
		PreparedStatement insertToStatement = connection
				.prepareStatement(insertToUserEdges);

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
	 * 
	 * @throws SQLException
	 */
	public void createUserEdgesTable3() throws SQLException {
		System.out.println("Creating User Edge table... Max tx:" + maxTx
				/ limit + " Starting at " + new Date());

		String sqlFindFromPubKey = "Select c.entity_id, b.tx_id from txout a inner join txin b on a.txout_id = b.txout_id  inner join light_entity_index c on a.pubkey_id = c.pub_key where a.tx_id > ? and a.tx_id <= ? and b.txout_id != 0 ";
		String sqlFindToPubKey = "Select b.entity_id, a.tx_id, a.txout_value from txout a inner join light_entity_index b on a.pubkey_id = b.pub_key where a.tx_id > ? and a.tx_id <= ?  ";
		String insertToUserEdges = "INSERT INTO user_edge (to_user, id, value) values (?, ?, ?) ";
		String insertFromUserEdges = "UPDATE user_edge  set from_user = ? where id = ? ";

		PreparedStatement fromStatement = connection.prepareStatement(sqlFindFromPubKey);
		PreparedStatement toStatement = connection.prepareStatement(sqlFindToPubKey);
		PreparedStatement insertFromStatement = connection.prepareStatement(insertFromUserEdges);
		PreparedStatement insertToStatement = connection.prepareStatement(insertToUserEdges);
		int interval = 100;
		int minBound = 0;
		int divs = maxTx / interval;
		int maxBound = divs;
		for (int i=0; i<= interval; i++) {
			int from = 0, to = 0, tx = 0;
			Long value = 0L;
			
			System.out.println("Searching for txs between " + minBound +" and " + maxBound);
			fromStatement.setInt(1, minBound);
			fromStatement.setInt(2, maxBound);
			toStatement.setInt(1, minBound);
			toStatement.setInt(2, maxBound);

			minBound += divs;
			maxBound += divs;
			rs = toStatement.executeQuery();
			while (rs.next()) {
				to = rs.getInt(1);
				tx = rs.getInt(2);
				value = rs.getLong(3);
				insertToStatement.setInt(1, to);
				insertToStatement.setInt(2, tx);
				insertToStatement.setLong(3, (value == null ? 0 : value));
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
	public void readDataFile() {}

	public void findBounds(String table, String column) throws SQLException {
		System.out.println(java.lang.Runtime.getRuntime().maxMemory());
		connection = DBConnection.get().connectPostgre();
		String stats = "select min(" + column + "), max(" + column + ") from "
				+ table;
		preparedStatement = connection.prepareStatement(stats);
		ResultSet rs = preparedStatement.executeQuery();
		while (rs.next()) {
			minTx = rs.getInt(1);
			maxTx = rs.getInt(2);
			System.out.println("Scanning from " + minTx + " tx to " + maxTx
					/ limit + " " + column + " from table " + table);
		}
	}

	public void findInsertedTxs() throws SQLException {
		connection = DBConnection.get().connectPostgre();
		String stats = "select count(id) from " + entity;
		preparedStatement = connection.prepareStatement(stats);
		ResultSet rs = preparedStatement.executeQuery();
		while (rs.next()) {
			numInsertedTxs = rs.getInt(1);
			System.out.println(numInsertedTxs + " entities inserted into "
					+ entity + " table");
		}
	}

	/**
	 * Sets clustered entity id for advance shadow public keys
	 */
	public void setClusteredEntityId() {
		String select = "SELECT min(b.pubkey_id), min(a.pubkey_id), min(c.entity_id) FROM advance_shadow a INNER JOIN txout b ON a.tx_id = b.tx_id "
				+ "INNER JOIN light_entity_index c ON b.pubkey_id = c.pub_key "
				+ "WHERE a.txout_pos != b.txout_pos GROUP BY b.tx_id";
		try {
			preparedStatement = connection.prepareStatement(select);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				Integer txoutId = rs.getInt(1);
				Integer advanceId = rs.getInt(2);
				Integer clusteredEntityId = rs.getInt(3);
				DBDataUpdater.updateInteger("light_entity_index", "clustered_entity_id", clusteredEntityId, " pub_key = " + advanceId);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void start() {
		try {
			findBounds("txin", "txin_id");
			// lightGroupTxsPubkeys();
			// createEntityTable();
			// lightCreateEntityIndexTable();
			 createUserEdgesTable3();
			// insertOnlyInputsAtEntityIndex();
			// insertAdvanceShadowAtEntityIndex();
			//setClusteredEntityId();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getNextException());
		}
	}
}
