package clasiffier;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import clasiffier.ListAddress.Cluster;
import database.DBConnection;

import abe.Config;
import bitcointools.Key;

/**
 * Classifier Manager that integrates with DB. Manager is responsible for updating DB
 * providing the community in which every entity participates.
 *
 */
public class ClassifierDBManager extends Key {
	
		private Connection connection;
		public ClassifierDBManager(){
			table = "light_entity_index";
			batchSize = 50;
		}
		
		public void readDataFile() { }
		
		public void insertListToDB(List<Integer> addressList, Cluster cluster){
			try {
				connection = DBConnection.get().connectPostgre();
				String insertTableSQL = "UPDATE " + table
						+ "SET " + cluster.name() + " = true WHERE  pub_key = ? ";
				preparedStatement = connection
						.prepareStatement(insertTableSQL);
				int rows = 0;
				for (Integer listId : addressList) {
					preparedStatement.setInt(1, listId);		
					preparedStatement.addBatch();
					if (rows % batchSize == 0) {
						if(Config.isDBIntegration()){
							addToDatabase(rows, preparedStatement);
						}
					}
					rows++;
				}
				preparedStatement.executeBatch();
				System.out.println(rows + " Records added to "+ cluster.name());
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e.getMessage());
			}
		
		}
		
		public void insertToDB(Cluster cluster){
			if (ListAddress.isComplexCluster())	
				insertComplexClusterToDB(cluster);
			else
				insertSimpleCLusterToDB(cluster);					
		}
		
		public void insertSimpleCLusterToDB(Cluster cluster){
			try {
				connection = DBConnection.get().connectPostgre();
				String insertTableSQL = "UPDATE " + table
						+ " SET " + cluster.name() + " = true WHERE  pub_key = ? ";
				preparedStatement = connection
						.prepareStatement(insertTableSQL);
				int rows = 0;
				for (ListAddress addr : ListManager.getListAddresses()) {
					preparedStatement.setInt(1, addr.getId());		
					preparedStatement.addBatch();
					if (rows % batchSize == 0) {
						if(Config.isDBIntegration()){
							addToDatabase(rows, preparedStatement);
						}
					}
					rows++;
				}
				preparedStatement.executeBatch();
				System.out.println(rows + " Addresses added to "+ cluster.name());
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e.getMessage());
			}
		}
		
		public void insertComplexClusterToDB(Cluster cluster){
			try {
				connection = DBConnection.get().connectPostgre();
				String insertTableSQL = "UPDATE " + table
						+ " SET " + cluster.name() + " = ? WHERE  pub_key = ? ";
				preparedStatement = connection
						.prepareStatement(insertTableSQL);
				int rows = 0;
				for (ListAddress addr : ListManager.getListAddresses()) {
					preparedStatement.setString(1, (addr.getValue() != null ? addr.getValue() : "none"));
					preparedStatement.setInt(2, addr.getId());		
					preparedStatement.addBatch();
					if (rows % batchSize == 0) {
						if(Config.isDBIntegration()){
							addToDatabase(rows, preparedStatement);
						}
					}
					rows++;
				}
				preparedStatement.executeBatch();
				System.out.println(rows + " Addresses added to "+ cluster.name());
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e.getMessage());
			}
		
		}
		
		public List<Integer> getIdsFromHashes(List<ListAddress> addressList) {
			List<Integer> idList = new ArrayList<Integer>();
			
			connection = DBConnection.get().connectPostgre();
			String selectSQL = "SELECT pubkey_id FROM pubkey WHERE  pubkey_hash = ? ";
			try {
				preparedStatement = connection
						.prepareStatement(selectSQL);
				for (ListAddress address : addressList) {
					preparedStatement.setString(1, address.getAddress());
					ResultSet rs = preparedStatement.executeQuery();
					while (rs.next()) {
						idList.add(rs.getInt(1));
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return idList;
		}

}
