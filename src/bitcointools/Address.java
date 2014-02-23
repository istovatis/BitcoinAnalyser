package bitcointools;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

import argo.jdom.JsonNode;
import argo.jdom.JsonStringNode;

public class Address {
	private int n;
	private String address;
	private double value;
	private int tx_index;
	private int type;
	private boolean input; // This address is input in a transaction
	private boolean output; // This address is output in a transaction
	private String addr_tag; // Some addresses have a tag
	private static int tagCount = 0;
	private static int genericCount = 0;

	private static ArrayList<String> inTags = new ArrayList<String>();
	private static ArrayList<String> outTags = new ArrayList<String>();

	private static HashSet<String> inHashTags = new HashSet<String>();
	private static HashSet<String> outHashTags = new HashSet<String>();
	private static HashSet<String> noHashTags = new HashSet<String>();

	public boolean isOutput() {
		return output;
	}

	public void setOutput(boolean output) {
		this.output = output;
	}
	
	public String getAddress(){
		return address;
	}

	public boolean isInput() {
		return input;
	}

	public void setInput(boolean input) {
		this.input = input;
	}
	
	public static int getGenericCount(){
		return genericCount;
	}

	public boolean createAddrFromJson(String name, String value) {
		if (name.equals("n")) {
			this.n = Integer.valueOf(value);
		} else if (name.equals("value")) {
			this.value = Double.valueOf(value);
		} else if (name.equals("addr")) {
			this.address = value;
		} else if (name.equals("tx_index")) {
			this.tx_index = Integer.valueOf(value);
		} else if (name.equals("type")) {
			this.type = Integer.valueOf(value);
		} else if (name.equals("addr_tag")) {
			// System.out.println(value+" "+input+" "+output);
			if (input && !output) {
				inTags.add(value);
				inHashTags.add(value);
			} else if (!input && output) {
				outTags.add(value);
				outHashTags.add(value);
			} else if (!input && !output) {
				System.out.println("Not input nor output set!");
			}
			tagCount++;
			addr_tag = value;
			return true;
		}
		if(input){
			genericCount++;
			//showInfo();
		}
	
		return false;
	}

	public void showInfo() {
		System.out.println("Address:");
		System.out.println("Tag: " + addr_tag);
		System.out.println("Address: " + address);
		System.out.println("Input: " + input + " Output: " + output);
	}

	public static void showTagStats() {
		System.out.println(Transaction.txs.size() + " Transactions loaded");
		System.out.println("Number of InTags :" + inTags.size()
				+ " Unique in Tags " + inHashTags.size());
		System.out.println("Number of OutTags :" + outTags.size()
				+ " Unique out Tags " + outHashTags.size());
		System.out.println("Tags counted: " + tagCount);
		System.out.println("Hashed Tagged addresses: "
				+ Transaction.numTaggedTxs());
	}

	public static void showHashTags() {
		System.out.println("---In Hash Tags---");
		for (String in : inHashTags) {
			System.out.println(in);
		}
		System.out.println();
		System.out.println("---Out Hash Tags---");
		for (String in : outHashTags) {
			System.out.println(in);
		}
	}

}
