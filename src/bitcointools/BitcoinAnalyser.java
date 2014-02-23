package bitcointools;
import java.sql.Connection;

import abe.ShadowClustering;
import abe.UserClustering1;


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
		
		BlockchainParser blockchain = new BlockchainParser();
		
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
		userClustering1.findBounds("txin");
		userClustering1.clusterTxs();
		userClustering1.insertUsersToDB();
		//userClustering1.clusterTxsFinal();
		
		ShadowClustering shadowClustering = new ShadowClustering();
//		shadowClustering.eliminateCoinGens("block_tx");
//		shadowClustering.eliminateOtherThanTwoOutputs();
//		shadowClustering.findBounds("txout");
//		shadowClustering.clusterTxs();
		
		try {
			//blockchain.homePage();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
