import java.util.ArrayList;

/**
 * Documentation from https://github.com/harrigan/bitcointools: This class is responsible for lengthy text keys for public keys (i.e.
 * “1EnHwdiKxvTE5AzcSnZqS52mMcHSLtCLwH”) and transaction keys (i.e.
 * “5dc77144dcf46a7f76e369d406481e857be9e95b21375935832a5bed4e23633b”).
 * 
 * Each ‘key’ field is a line number (starting at 1) to index into the
 * appropriate list file. Using this, information on a public key or specific
 * transaction can be queried on sites such as blockchain.info.
 * 
 * 
 * @author istovatis
 * 
 */
public abstract class Key extends HasParser{
	String key;
	ArrayList<Key> keyList;
	@Override
	public void readDataFile() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public int emptyTable() {
		return super.emptyTable();
	}
}
