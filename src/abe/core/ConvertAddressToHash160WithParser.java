package abe.core;

import java.util.List;

import parser.WebPageParser;

import database.DBDataReceiver;
import database.DBDataUpdater;


/**
 * 
 * Converts an address to Hash160 using blockchain api.
 * @author istovatis
 *
 */
public class ConvertAddressToHash160WithParser {
	private final static String page = "https://blockchain.info/q/addresstohash/";
	
	public static String convertAddressToHash160(String address) {
		try {
		    Thread.sleep(500);
		} catch(InterruptedException ex) {
		    Thread.currentThread().interrupt();
		}
		return WebPageParser.parseWholeTextPage(page+address);
	}
	
	public static void convertColumnAddressToHash160(String field, String table) {
		List<String> addresses = (List<String>) DBDataReceiver.selectStringWhereClause(field, table, "bit_length("+ field +") != 320");
		System.out.println("Converting " + addresses.size() + " addresses into HASH 160");
		for (String address : addresses) {
			DBDataUpdater.updateString(table, field, ConvertAddressToHash160WithParser.convertAddressToHash160(address), field + " = '" + address + "' ;");
		}
	}
	
	public static void main(String[] args) {
		convertColumnAddressToHash160("address", "donation");
	}
}
