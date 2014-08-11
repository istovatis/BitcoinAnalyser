package bitcointools;
import java.sql.Connection;

import clasiffier.Donations;
import abe.Config;
import abe.analyser.SavingAccounts;
import abe.user_clustering.ShadowClustering;
import abe.user_clustering.InputUserClustering;
import abe.user_clustering.UserEdge;


public class BitcoinAnalyser {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Config.setDBIntegration(true);
		Config.setDublicatesIntegration(false);
		
		UserEdge userEdge = new UserEdge();
		//userEdge.readDataFile();
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
		
		InputUserClustering userClustering1 = new InputUserClustering();
		//userClustering1.start();
		
		SavingAccounts.start();
		
		ShadowClustering shadowClustering = new ShadowClustering();
		//shadowClustering.start();
		
	}

}
