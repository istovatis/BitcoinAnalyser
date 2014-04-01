package clasiffier;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import clasiffier.ListAddress.Cluster;

public class Classifier {
	public static void main(String[] args) {

		for (String input : args) {
			if (!classify(input))
				System.out.println("There is no classifier named "+input+ ". Please check the name you provided");
		}
	}
	
	public static boolean classify(String classy) {
		for (Cluster cluster : ListAddress.Cluster.values()) {
			if (classy.equals(cluster.name())) {
				ClassifierDBManager clusterManager = new ClassifierDBManager();
				List<Integer> listIds = clusterManager.getIdsFromHashes(ManagerRunner(cluster));		
				clusterManager.insertToDB(cluster);
				return true;
			}
		}
		return false;
	}
	
	public static List<ListAddress> ManagerRunner(Cluster cluster) {
		System.out.println("Starting "+cluster.name()+" clustering");
		ListManager manager = new ListManager(cluster);
		manager.generate(cluster);
		System.out.println(manager.getListAddresses().size()+". pubkey hashes loaded. Pubkey ids stored from thoses hashes.");
		return manager.getListAddresses();
	}
}
