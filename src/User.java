/**
 * Documentation from https://github.com/harrigan/bitcointools: This class
 * organizes the ‘user’ information, where a ‘user’ is a grouping of public keys
 * inferred from public keys combined as inputs into a single transaction
 * (meaning the user owns the private key to each address). This method is
 * described in (Reid and Harrigan, 2011). Briefly, we create a graph where two
 * public keys have an edge if they have been used as inputs in a single
 * transaction. The connected components of this graph are‘users’. Each line in
 * “userkey_list.txt” is one of these components (a grouping of public keys).
 * The files “user_edge_inputs.txt” and “user_edge_inputs_public_keys.txt”
 * record the transaction keys, and public keys used as input to “this”
 * transaction (see Figure 1).
 * 
 * 
 * @author istovatis
 * 
 */
public interface User {
	
}
