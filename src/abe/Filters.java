package abe;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;

import abe.user_clustering.ShadowClustering;
import database.DBConnection;
import database.DBInteraction;


public class Filters extends DBInteraction{
	private static HashSet<Integer> notCoinGenTx;
	int minTx, maxTx;

	/**
	 * Find txs that are not coin generations
	 * 
	 * @param table
	 * @return 
	 */
	public HashSet<Integer> eliminateCoinGens(int limit) {
		notCoinGenTx = new HashSet<Integer>();
		connection = DBConnection.get().connectPostgre();
		String stats = "select tx_id from block_tx where tx_pos != 0 order by tx_id limit " + limit;
		try {
			preparedStatement = connection.prepareStatement(stats);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				notCoinGenTx.add(rs.getInt(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		finally {
			if (connection != null)
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}
		System.out.println(notCoinGenTx.size() + " Transactions that are not coin generations loaded");
		return notCoinGenTx;
	}
	
	/**
	 * Find txs that are not coin generations and not containing at least one output that is also an input.
	 * 
	 * @param table
	 * @return 
	 */
	public HashSet<Integer> eliminateCoinGensOrSameInOuts(int limit) {
		notCoinGenTx = new HashSet<Integer>();
		connection = DBConnection.get().connectPostgre();
		String stats = ShadowClustering.notNewGenNotOutputAtInput() + " limit " + limit;
		try {
			preparedStatement = connection.prepareStatement(stats);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				notCoinGenTx.add(rs.getInt(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		finally {
			if (connection != null)
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}
		System.out.println(notCoinGenTx.size() + " Transactions that are not coin generations and not containing at least one output that is also an input loaded");
		return notCoinGenTx;
	}
	
	/**
	 * Eliminate txs that contain other than two outputs
	 * @param txs The set from which other than two outputs are eliminated
	 * @return	The set containing only two outputs
	 */
	public HashSet<Integer> eliminateOtherThanTwoOutputs(HashSet<Integer> txs){
		connection = DBConnection.get().connectPostgre();
		Iterator<Integer> it = txs.iterator();
		int size = txs.size();
		int i = 0;
		while (it.hasNext()) {
			String stats = "select count(txout_id) from txout where tx_id = "+it.next();
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
			i++;
			if (i % size == 1000)
				System.out.println(i+" scanned at "+new Date());
		}	
		System.out.println(txs.size() + " Transactions contain two outputs");
		
		return txs;
	}
	
	public void findBounds(String table) {
		connection = DBConnection.get().connectPostgre();
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
	
	public int getMax() {
		return maxTx;
	}
	
	public int getMin() {
		return minTx;
	}

	@Override
	public void readDataFile() {}
}
