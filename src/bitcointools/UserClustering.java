package bitcointools;
/**
 * Detect shadow addresses at bitcoin. Shadow addresses are generated by bitcoin
 * so as to return the changes to the user that made the transaction. Find all
 * transactions with two output addresses, and search for addresses' history at
 * blockchain. If on of them is pretty new, then we can assume that this is
 * address is generated by bitcoin to give changes back to the user.
 * 
 * @author Istovatis -- istovatis@gmail.com --
 * 
 */
public class UserClustering {
		// Take all transaction hashes from transactionkey_list.txt. Make a request to blockchain.
	
		// If the blockchain reports two output addresses, 
}
