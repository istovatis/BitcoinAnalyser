package clasiffier.mining_pool;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;

import parser.WebPageParser;


import abe.Utils;
import abe.core.ConvertAddressToHash160WithParser;

import com.gargoylesoftware.htmlunit.html.HtmlAnchor;
import com.gargoylesoftware.htmlunit.html.HtmlTableDataCell;

import database.DBDataProvider;
import database.DBDataReceiver;
import database.DBDataUpdater;

/**
 * Having a block, the aim is to find the award receiver address. To do that,
 * parse blockchain info page for a given block, and get the output address of
 * the first transaction. Save this address to mining pool table at database.
 * 
 * The power of this method is that it is easy to get award winners for blocks
 * that don't exist in database, but the receiver exists (has participated in a
 * transaction before block generation).
 * 
 * @author istovatis
 * 
 */

public class AwardReceiverFinder {
	private static Map<String, Integer> blockToPubKey = new HashMap<String, Integer>();	// Map block hash with receiver pub key 	
	private final String blockChain = "https://blockchain.info/block/"; // the
																		// webpage
	private String xpath = "//td[@class='txtd']//a";

	public String getReceiverAddress(String block) throws Exception {
		System.out.println(block + " : ");
		List<?> list = WebPageParser.parseWebPage(blockChain + block, xpath);
		if (list.size() > 0) {
			HtmlAnchor cell = (HtmlAnchor) list.get(0);
			return ConvertAddressToHash160WithParser.convertAddressToHash160(cell.getTextContent());
		}
		return "";
	}
	
	public void addWinnerPubKey(String block, String pubkey_hash) {
			String where = "pubkey_hash = '" + pubkey_hash + "'";
			ArrayList<Integer> pubkey_ids = new ArrayList<Integer>();
			pubkey_ids.addAll((ArrayList<Integer>) DBDataReceiver.selectIntegerWhereClause("pubkey_id", "pubkey", where));
			for (Integer pubkeyId : pubkey_ids) {
				String blockWhere = "block_hash = '" + block + "'";
				DBDataUpdater.updateInteger("mining_pool", "pubkey_id", pubkeyId, blockWhere);
			}
	}

	public static void main(String[] args) {
		try {
			System.out.println("Add pubkey ids from block winners at Database");
			AwardReceiverFinder finder = new AwardReceiverFinder();
			ArrayList<String> blocks = (ArrayList<String>) DBDataReceiver.selectStringClause("block_hash",
					"mining_pool");
			for (String block : blocks)
				finder.addWinnerPubKey(block, finder.getReceiverAddress(block));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Having all potential miners (receivers of generation coin), get all addresses that have received a tx from initial receivers
	 * @return
	 */
	public static String getAllPotentialMiners() {
		return "SELECT pubkey_hash FROM pubkey WHERE pubkey_id in " +
				"(SELECT pubkey_id FROM txout WHERE tx_id in " +
				"(SELECT txin.tx_id FROM txin  inner join txout on txin.txout_id = txout.txout_id inner join pubkey on txout.pubkey_id = pubkey.pubkey_id WHERE  txout.pubkey_id in" +
				"(SELECT pubkey_id FROM mining_pool WHERE pubkey_id IS NOT NULL)))";
	}
	
	/**
	 * Get all txs from generated Coins that gained by mining pool addresses
	 * @return
	 */
	public static String getAllTxsWhereWinnerIsMiningPool() {
		return "SELECT tx_id from txin where txout_id is null and tx_id in (SELECT tx_id from txout where pubkey_id in (SELECT pubkey_id from mining_pool where pubkey_id IS NOT NULL))";
	}
}