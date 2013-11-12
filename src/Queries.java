
public class Queries {
	
	private static String sql;
	
	public static String duplicateTxHashes(){
		 //select  distinct( transactionkey_list.transaction_key_string) from user_edge_inputs inner join user_edges_duplicates on user_edges_duplicates.transaction_key = user_edge_inputs.transaction_key inner join transactionkey_list on  transactionkey_list.id = user_edge_inputs.line
		sql = "select transaction_key_string from transactionkey_list inner join user_edges_duplicates on user_edges_duplicates.line = transactionkey_list.id";
		  return sql;
	}
}
