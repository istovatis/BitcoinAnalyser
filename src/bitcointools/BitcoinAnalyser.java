package bitcointools;
import java.sql.Connection;

import clasiffier.Donations;

import abe.Config;
import abe.user_clustering.ShadowClustering;
import abe.user_clustering.UserClustering1;


public class BitcoinAnalyser {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Config.setDBIntegration(true);
		Config.setDublicatesIntegration(false);
		
		UserEdges userEdges = new UserEdges();
		//userEdges.readDataFile();
		//userEdges.matchTxsWithHashes();
		//userEdges.addNewLine();
		//userEdges.seperateFiles();
		//userEdges.countSplitFilesLines();
		
		PublicKeyList publicKey = new PublicKeyList();
		//publicKey.readDataFile();
		
		UserKeyList userKeyList = new UserKeyList();
		//userKeyList.readDataFile();
		
		TransactionKeyList transactionKeyList = new TransactionKeyList();
		//transactionKeyList.readDataFile();
		
		UserEdgeInputsPublicKeys userEdgeInputsPublicKeys = new UserEdgeInputsPublicKeys();
		//userEdgeInputsPublicKeys.readDataFile();
		
		UserEdgeInputs userEdgeInputs = new UserEdgeInputs();
		//userEdgeInputs.readDataFile();
		
		Duplicates duplicates = new Duplicates();
		//duplicates.readDataFile();
		
		Transactions transactions = new Transactions();
		//transactions.readDataFile();
		
		RemoveNewGens removeNewGens = new RemoveNewGens();
		//removeNewGens.readDataFile();
		
		UserClustering1 userClustering1 = new UserClustering1();
		userClustering1.start();
		
		ShadowClustering shadowClustering = new ShadowClustering();
		//shadowClustering.start();
		
	}

}
