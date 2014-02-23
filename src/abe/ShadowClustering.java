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

import bitcointools.Database;
import bitcointools.HasParser;

public class ShadowClustering extends HasParser {
	HashSet<Integer> notNewGenTx;
	ArrayList<HashSet<Integer>> groupedTxs;
	HashSet<Integer> addedPubKey;
	ResultSet rs;
	HashSet<Integer> currentTx;
	int minTx, maxTx;
	int moreThanOneShadows = 0; //num of txs that contain more than one shadow

	public ShadowClustering() {
		table = "entity";
	}

	public void clusterTxs() {
		try {
			groupedTxs = new ArrayList<HashSet<Integer>>();
			Map<Integer, Integer> txToUser = new HashMap<Integer, Integer>();
			addedPubKey = new HashSet<Integer>();
			Statement stSelect = connection.createStatement();
			for (int i = minTx; i < maxTx; i++) {
				String sql = "SELECT tx_id, txout_id, pubkey_id FROM txout WHERE txout_id = " + i;
				rs = stSelect.executeQuery(sql);
				

				int selectedSet = -1;
				while (rs.next()) {
					int tx_id = rs.getInt(1);
					int txout_id = rs.getInt(2);
					int pubkey_id = rs.getInt(3);
					// There is only one shadow at every tx
					if(!notNewGenTx.contains(tx_id)){
						notNewGenTx.add(tx_id);
						// This is the 1st appearance of the key
						if(!addedPubKey.contains(pubkey_id)) {
							ShadowAddress.shadowAddresses.add(new ShadowAddress(tx_id, txout_id, pubkey_id));
							addedPubKey.add(pubkey_id);
						}
					} 
				}
				rs.close();

				if (selectedSet != -1) {
					groupedTxs.get(selectedSet).addAll(currentTx);
					for (int key : currentTx)
						txToUser.put(key, selectedSet);
				} else {
					for (int key : currentTx)
						txToUser.put(key, groupedTxs.size());
					groupedTxs.add(currentTx);
				}
				if (i % 500000 == 0)
					showInfo(i);
			}
			// for (HashSet set : groupedTxs) {
			// if (set.size() > 4) {
			// Iterator iterator = set.iterator();
			//
			// // check values
			// while (iterator.hasNext()) {
			// System.out.print(": " + iterator.next() + " ");
			// }
			// System.out.println();
			// }
			// }
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void insertUsersToDB() {
		try {
			String insertTableSQL = "INSERT INTO " + table
					+ "(id, tx_ids) VALUES" + "(?,?)";
			preparedStatement = connection.prepareStatement(insertTableSQL);

			for (int i = 0; i < groupedTxs.size(); i++) {
				preparedStatement.setInt(1, i + 1);
				Integer[] tmp = groupedTxs.get(i).toArray(new Integer[0]);
				Array intArray = connection.createArrayOf("integer", tmp);
				preparedStatement.setArray(2, intArray);
				preparedStatement.addBatch();
				if (i % 100000 == 0) {
					addToDatabase(i, preparedStatement);
				}
			}
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

	/**
	 * Find txs that are not coin generations
	 * 
	 * @param table
	 */
	public void eliminateCoinGens(String table) {
		notNewGenTx = new HashSet<Integer>();
		connection = Database.get().connectPostgre();
		String stats = "select tx_id from " + table + " where tx_pos != 0";
		try {
			preparedStatement = connection.prepareStatement(stats);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				notNewGenTx.add(rs.getInt(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println(notNewGenTx + " Transactions that are not coin generations loaded");
	}
	
	/**
	 * Eliminate txs that contain other than two outputs
	 */
	public void eliminateOtherThanTwoOutputs(){
		connection = Database.get().connectPostgre();
		table = "txout";
		Iterator<Integer> it = notNewGenTx.iterator();
		while (it.hasNext()) {
			String stats = "select count(txout_id) from " + table + " where tx_id = "+it.next();
			try {
				preparedStatement = connection.prepareStatement(stats);
				ResultSet rs = preparedStatement.executeQuery();
				while (rs.next()) {
					if(rs.getInt(1) != 2)
						it.remove();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}	
		}	
		System.out.println(notNewGenTx + " Transactions contain two outputs");
	}

	public void findBounds(String table) {
		connection = Database.get().connectPostgre();
		String stats = "select min(txout_id), max(txout_id) from " + table;
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
}
