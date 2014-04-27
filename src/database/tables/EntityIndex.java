package database.tables;

import database.DBDataUpdater;

/**
 * Query execution at Entity Index table
 * @author Istovatis
 *
 */
public class EntityIndex {

	public static void setDonationTrue() {
		System.out.println("Reading donation table and setting donation fields of ENTITY INDEX table to true ");
		String query = "UPDATE entity_index SET donation = true WHERE pub_key in (SELECT pubkey.pubkey_id FROM pubkey INNER JOIN donation on pubkey.hash = donation.address)";
		DBDataUpdater.simpleUpdate(query);
	}
	
	public static void setMininPoolTrue() {
		System.out.println("Reading mining pool table and setting mining pool fields of ENTITY INDEX table to true ");
		String query = "UPDATE entity_index SET mining_pool = true WHERE pub_key in (SELECT mining_pool.pubkey_id FROM mining_pool)";
		DBDataUpdater.simpleUpdate(query);
	}
	
	public static void main(String[] args) {
		//setDonationTrue();
		setMininPoolTrue();
	}
	
}
