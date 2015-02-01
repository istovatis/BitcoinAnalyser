package database;

/**
 * Useful predefined queries.
 *
 */
public class Queries {

	private static String sql;

	public static String duplicateTxHashes() {
		sql = "SELECT transaction_key_string "
				+ " FROM transactionkey_list "
				+ " INNER JOIN user_edges_duplicates ON"
				+ " user_edges_duplicates.line = transactionkey_list.id";
		return sql;
	}

	public static String getTxHashString(Integer id) {
		sql = "SELECT transaction_key_string "
			+ " FROM transactionkey_list "
			+ " WHERE id = " + id;
		return sql;
	}

	/**
	 * Give a tag. Get all the public keys associated with this tag!
	 * 
	 * @param tag
	 * @return
	 */
	public static String getTagPubKeys(String tag) {
		sql = "SELECT tags.tag, userkey_list.public_keys "
			+ " FROM tags "
			+ " INNER JOIN pubkey_list "
			+ " ON tags.address = pubkey_list.text_key AND"
			+ " tags.tag like '"
			+ tag + "%'+ INNER JOIN userkey_list ON userkey_list.key_number = pubkey_list.id";
		return sql;
	}

	public static String selectAllTxIDs() {
		return "SELECT txout.txout_pubkey_id FROM tx inner join txout where tx.tx";
	}

	public static String maxEntityLength() {
		return "SELECT MAX(array_length(pub_keys, 1)) FROM entity";
	}

	public static String avgEntityLength() {
		return "SELECT AVG(array_length(pub_keys, 1)) FROM entity";
	}

	/**
	 * Returns the public key hash string of every input of a given tx
	 * 
	 * @return
	 */
	public static String pubkeyHashOfTxInputs() {
		return "SELECT pubkey.pubkey_hash "
			+ " FROM txin "
			+ " INNER JOIN txout "
			+ " ON txin.txout_id = txout.txout_id "
			+ " INNER JOIN pubkey ON txout.pubkey_id = pubkey.pubkey_id "
			+ " WHERE  txin.tx_id = ?";
	}
	
	/**
	 * Retun the pubkeys of txins used a specific tx_id
	 * 
	 * @return
	 */
	public static String pubkeysIdOfTxInputs() {
		return " SELECT txout.pubkey_id "
			+ " FROM txin LEFT JOIN txout ON ("
			+ " txout.txout_id = txin.txout_id) "
			+ "WHERE txin.tx_id = ?  ";
	}

	public static String outputAddressHashes() {
		return "SELECT * "
			+ " FROM txout "
			+ " WHERE tx_id IN"
			+ " (SELECT txin.tx_id"
			+ " FROM txin  inner join txout ON txin.txout_id = txout.txout_id "
			+ " INNER JOIN pubkey ON txout.pubkey_id = pubkey.pubkey_id "
			+ " WHERE  txout.pubkey_id = ?)";
	}	
	
	public static String setTxoutPosToAdvanceIndex() {
		return "UPDATE advance_shadow "
			+ " SET txout_pos = txout.txout_pos "
			+ " FROM txout "
			+ " WHERE advance_shadow.pubkey_id = txout.pubkey_id";
	}

}
