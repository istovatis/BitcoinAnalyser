import java.io.File;


public interface HasParser {
	final static String path = System.getProperty("user.home")+File.separator+"Dropbox/development/bitcoin/bitcoin_uic_data_and_code_20130107/bitcoin_network_blockchain_215529/"; //System.getProperty("user.dir") + File.separator;
	public void readDataFile();
}
