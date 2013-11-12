import java.sql.Connection;


public class BitcoinAnalyser {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Config.setDBIntegration(true);
		Config.setDublicatesIntegration(false);
		
		UserEdges userEdges = new UserEdges();
		//userEdges.readDataFile();
		
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
		duplicates.readDataFile();
		
		try {
			//blockchain.homePage();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
