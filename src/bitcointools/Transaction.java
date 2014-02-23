package bitcointools;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import argo.jdom.JsonField;
import argo.jdom.JsonNode;
import argo.jdom.JsonNodeSelector;
import argo.jdom.JsonStringNode;

public class Transaction {
	private Integer blockHeight; // Number of Blocks this transaction is
									// included
	private String time;
	private Integer id; 	//the id contained in dataset
	private String ip; // Ip address that broadcast this transaction to
						// blockchain.info first
	private int size;
	private int inNum; // Number of input addresses
	private int outNum; // Number of output addresses
	private int tx_index;
	private int ver;
	private String hash; // The hash of the transaction
	private boolean hasTagedAddress; // Set to true if even one address involved is tagged
	private HashSet<Address> inAddress;
	private HashSet<Address> outAddress;
	public static ArrayList<Transaction> txs = new ArrayList<Transaction>();
	private static HashSet<String> taggedTxs = new HashSet<String>();	//store the hash of a tx with tagged addresses
	public static HashSet<Integer> txIds = new HashSet<Integer>();	//store all tx ids
	private AtomicInteger numRequests;	//Number of this tx requests
	private static AtomicInteger dummedRequests = new AtomicInteger();	//Number of this tx requests
	
	public Transaction() {
		inAddress = new HashSet<Address>();
		outAddress = new HashSet<Address>();
	}
	
	public HashSet<Address> getInAddresses(){
		return inAddress;
	}
	
	public HashSet<Address> getOutAddresses(){
		return outAddress;
	}
	
	/**
	 * Return the number of in and out addresses of the tx
	 * @return
	 */
	public int getNumAddresses(){
		return outAddress.size()+inAddress.size();
	}
	
	public Transaction(Integer id, String hash) {
		inAddress = new HashSet<Address>();
		outAddress = new HashSet<Address>();
		this.id = id;
		this.hash = hash;
		numRequests = new AtomicInteger();
	}
	
	public boolean isHasTagedAddress() {
		return hasTagedAddress;
	}

	public void setHasTagedAddress(boolean hasTagedAddress) {
		this.hasTagedAddress = hasTagedAddress;
	}
	
	public HashSet<String> getTaggedTxs(){
		return taggedTxs;
	}
	
	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
	}
	
	public Integer getId(){
		return id;
	}
	
	public int getNumRequests(){
		return numRequests.get();
	}
	
	public int getDummedRequests(){
		return dummedRequests.get();
	}
	
	public int oneMoreRequest(){
		dummedRequests.incrementAndGet();
		return numRequests.incrementAndGet();
	}

	/**
	 * Create transaction from json parsing.
	 */
	public void createTxFromJson(Map.Entry<JsonStringNode, JsonNode> entry) {
		if (entry.getKey().getText().equals("block_height")) {
			String value = entry.getValue().getText().toString();
			blockHeight = Integer.valueOf(value);
		} else if (entry.getKey().getText().equals("time")) {
			String value = entry.getValue().getText().toString();
			time = entry.getValue().getText().toString();
		} else if (entry.getKey().getText().equals("vout_sz")) {
			String value = entry.getValue().getText().toString();
			inNum = Integer.valueOf(value);
		} else if (entry.getKey().getText().equals("vin_sz")) {
			String value = entry.getValue().getText().toString();
			outNum = Integer.valueOf(value);
		} else if (entry.getKey().getText().equals("tx_index")) {
			String value = entry.getValue().getText().toString();
			tx_index = Integer.valueOf(value);
		} else if (entry.getKey().getText().equals("ver")) {
			String value = entry.getValue().getText().toString();
			ver = Integer.valueOf(value);
		} else if (entry.getKey().getText().equals("inputs")) {
			parseInAddr(entry);
		} else if (entry.getKey().getText().equals("vout_sz")) {
			List<JsonNode> elements = entry.getValue().getElements();
			for (JsonNode element : elements) {
				List<JsonField> el = element.getFieldList();
				for (JsonField e : el) {
					Address address = new Address();
					Map<JsonStringNode, JsonNode> list = e.getValue()
							.getFields();
					for (Map.Entry<JsonStringNode, JsonNode> in : list
							.entrySet()) {
						address.setInput(true);
						address.createAddrFromJson(in.getKey().getText(), in
								.getValue().getText());

					}
				}
			}
		}
		if (entry.getKey().getText().equals("relayed_by")) {
			this.ip = entry.getValue().getText();
			// System.out.println(this.ip+"___");
		}
		if (entry.getKey().getText().equals("hash")) {
			hash = entry.getValue().getText();
		}
		if (entry.getKey().getText().equals("out")) {
			parseOutAddr(entry);
		}
		// System.out.println(entry.getKey().getText()+"  * "+entry.getValue());
		// showInfo();
	}

	public void parseInAddr(Map.Entry<JsonStringNode, JsonNode> entry) {
		
		List<JsonNode> elements = entry.getValue().getElements();
		for (JsonNode element : elements) {
			Address address = new Address();
			List<JsonField> el = element.getFieldList();
			for (JsonField e : el) {
				 //System.out.println(e.getName()+" && "+e.getValue());
				Map<JsonStringNode, JsonNode> list = e.getValue().getFields();
				for (Map.Entry<JsonStringNode, JsonNode> in : list.entrySet()) {
					address.setInput(true);
					if (address.createAddrFromJson(in.getKey().getText(), in
							.getValue().getText())) {
						hasTagedAddress = true;
					}
					this.inAddress.add(address);
					 //System.out.println(in.getKey().getText()+"   "+in.getValue());
				}
			}
		}
	}

	public void parseAddr(Map.Entry<JsonStringNode, JsonNode> entry,
			boolean isInput) {
		List<JsonNode> elements = entry.getValue().getElements();
		for (JsonNode element : elements) {
			List<JsonField> el = element.getFieldList();
			for (JsonField e : el) {
				Address address = new Address();
				// System.out.println(e.getName()+" && "+e.getValue());
				Map<JsonStringNode, JsonNode> list = e.getValue().getFields();
				for (Map.Entry<JsonStringNode, JsonNode> in : list.entrySet()) {
					if (isInput) {
						address.setInput(true);
						this.inAddress.add(address);
					} else {
						address.setOutput(true);
						this.outAddress.add(address);
					}
					if (address.createAddrFromJson(in.getKey().getText(), in
							.getValue().getText())) {
						taggedTxs.add(this.hash);
						hasTagedAddress = true;
					}
					// System.out.println(in.getKey().getText()+"   "+in.getValue());
				}
			}
		}
	}

	public void parseOutAddr(Map.Entry<JsonStringNode, JsonNode> entry) {
		List<JsonNode> elements = entry.getValue().getElements();
		for (JsonNode element : elements) {
			List<JsonField> el = element.getFieldList();
			Address address = new Address();
			for (JsonField e : el) {
				
				address.setOutput(true);
				if (address.createAddrFromJson(e.getName().getText(), e
						.getValue().getText())) {
					hasTagedAddress = true;
				}
				this.outAddress.add(address);
			}
		}
	}

	public void showInfo() {
		System.out.println("**************");
		System.out.println("Transaction:");
		System.out.println("Hash: " + hash);
		System.out.println("Included In Blocks: " + blockHeight
				+ " Received Time: ");
		System.out.println("Relayed by IP: " + ip + " Input Addresses: "
				+ inNum + " Output Addresses: " + outNum);
		System.out.println("**************");
	}

	public void showMore() {
		System.out.println(txs.size() + " Transactions requested");
	}

	public static int numTaggedTxs() {
		return taggedTxs.size();
	}

}
