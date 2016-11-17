package bitcointools;


import abe.Config;
import abe.analyser.SavingAccounts;
import abe.user_clustering.ShadowClustering;
import abe.user_clustering.InputUserClustering;
import abe.user_clustering.UserEdge;

public class BitcoinAnalyser {

	public static void main(String[] args) {
		Config.setDBIntegration(true);
		Config.setDublicatesIntegration(false);

		if (args.length < 1) {
			showUsageInfo();
			System.exit(0);
		}
		String option = args[0];
		if (option.equals("-u")) {
			UserEdge userEdge = new UserEdge();
			userEdge.readDataFile();
		}

		if (option.equals("-d")) {
			Duplicates duplicates = new Duplicates();
			duplicates.readDataFile();
		}

		if (option.equals("-r")) {
			RemoveNewGens removeNewGens = new RemoveNewGens();
			removeNewGens.readDataFile();
		}

		if (option.equals("-sc")) {
			InputUserClustering userClustering1 = new InputUserClustering();
			userClustering1.start();
		}

		if (option.equals("-s")) {
			SavingAccounts.start();
		}

		if (option.equals("-sc")) {
			ShadowClustering shadowClustering = new ShadowClustering();
			shadowClustering.start();
		}
	}

	private static void showUsageInfo() {
		System.out.println("Usage:  -u for user Edge \n " +
				"-sc for shadow clustering \n " +
				" -d for duplicates \n " +
				" -us for user clustering \n " +
				" -s for Saving accounts");
	}

}
