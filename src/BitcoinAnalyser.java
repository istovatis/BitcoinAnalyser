import java.sql.Connection;


public class BitcoinAnalyser {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Config.setDBIntegration(false);
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
		
		Scraper scraper = new Scraper();
		System.out.println(scraper.excutePost("http://blockchain.info/rawtx/aee28d1bcabe6dea4cf5459d66f22b7e4e68ca23fdd4caca2ed00c276a79e85b?format=json", ""));
		scraper.parseJson();
		Transaction tx = new Transaction();
		tx.createTxFromJson(scraper.getMap());
		try {
			//blockchain.homePage();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
