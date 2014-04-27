package clasiffier;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import clasiffier.ListAddress.Cluster;
import clasiffier.mining_pool.MiningPool;

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
			if(classy.equals("donation"))
				return startDonationParser();
			if(classy.equals("miningPool"))
				return startMiningPool();
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
	
	public static boolean startDonationParser() {
		Donations blockchain = new Donations();
		try {
			blockchain.parserForDonations();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}
	
	public static boolean startMiningPool() {
		MiningPool miningPool = new MiningPool();
		try {
			miningPool.homePage();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}
}
