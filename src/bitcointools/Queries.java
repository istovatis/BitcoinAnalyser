package bitcointools;

public class Queries {
	
	private static String sql;
	
	public static String duplicateTxHashes(){
		 //select  distinct( transactionkey_list.transaction_key_string) from user_edge_inputs inner join user_edges_duplicates on user_edges_duplicates.transaction_key = user_edge_inputs.transaction_key inner join transactionkey_list on  transactionkey_list.id = user_edge_inputs.line
		sql = "select transaction_key_string from transactionkey_list inner join user_edges_duplicates on user_edges_duplicates.line = transactionkey_list.id";
		  return sql;
	}
	
	public static String getTxHashString(Integer id){
		sql = "SELECT transaction_key_string FROM transactionkey_list WHERE id = "+id;
		return sql;
	}
	
	/**
	 * Give a tag. Get all the public keys associated with this tag!
	 * @param tag
	 * @return
	 */
	public static String getTagPubKeys(String tag){
		// extra: show lines
		//sql = "select tags.tag, userkey_list.public_keys, array_length(userkey_list.public_keys, 1) as length from tags inner join pubkey_list on tags.address = pubkey_list.text_key and tags.tag like '"+tag+"%'+ inner join userkey_list on userkey_list.key_number = pubkey_list.id";
		sql = "select tags.tag, userkey_list.public_keys from tags inner join pubkey_list on tags.address = pubkey_list.text_key and tags.tag like '"+tag+"%'+ inner join userkey_list on userkey_list.key_number = pubkey_list.id";
		return sql;
	}
	
	public static String selectAllTxIDs(){
		return "SELECT txout.txout_pubkey_id from tx inner join txout where tx.tx";
	}
	
}
