import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import argo.jdom.JsonField;
import argo.jdom.JsonNode;
import argo.jdom.JsonNodeSelector;
import argo.jdom.JsonStringNode;

public class Transaction {
	private Integer blockHeight; // Number of Blocks this transaction is
									// included
	private String time;
	private String ip; // Ip address that broadcast this transaction to
						// blockchain.info first
	private int size;
	private int inNum; // Number of input addresses
	private int outNum; // Number of output addresses
	private int tx_index;
	private int ver;
	private String hash;		// The hash of the transaction
	ArrayList<Address> inAddress;
	ArrayList<Address> outAddress;
	public static ArrayList<Transaction> txs = new ArrayList<Transaction>();

	/**
	 * Create transaction from json parsing.
	 */
	public void createTxFromJson(Map<JsonStringNode, JsonNode> map) {
		inAddress = new ArrayList<Address>();
		outAddress = new ArrayList<Address>();
		for (Map.Entry<JsonStringNode, JsonNode> entry : map.entrySet()) {
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
			} 
			else if (entry.getKey().getText().equals("vout_sz")) {
				List<JsonNode> elements = entry.getValue().getElements();
				for(JsonNode element : elements){
					List<JsonField> el = element.getFieldList();
					for(JsonField e : el){
						Address address = new Address();
						Map<JsonStringNode, JsonNode> list = e.getValue().getFields();
						 for (Map.Entry<JsonStringNode, JsonNode> in :
							 list.entrySet()) {
							 address.setInput(true);
							 address.createAddrFromJson(in.getKey().getText(), in.getValue().getText());
						 }
					}
				}
			} 
			if(entry.getKey().getText().equals("relayed_by")){
				 this.ip = entry.getValue().getText();
				 // System.out.println(this.ip+"___");
			 }
			 if(entry.getKey().getText().equals("hash")){
				 this.hash = entry.getValue().getText();
			 }
			 if(entry.getKey().getText().equals("out")){
				 parseOutAddr(entry);
			 }
			 //System.out.println(entry.getKey().getText()+"  * "+entry.getValue());
		}
		txs.add(this);
		//showInfo();
	}
	
	public void parseInAddr(Map.Entry<JsonStringNode, JsonNode> entry){
		List<JsonNode> elements = entry.getValue().getElements();
		for(JsonNode element : elements){
			List<JsonField> el = element.getFieldList();
			for(JsonField e : el){
				Address address = new Address();
				//System.out.println(e.getName()+" && "+e.getValue());
				Map<JsonStringNode, JsonNode> list = e.getValue().getFields();
				 for (Map.Entry<JsonStringNode, JsonNode> in :
					 list.entrySet()) {
					 address.setInput(true);
					 address.createAddrFromJson(in.getKey().getText(), in.getValue().getText());
					 this.inAddress.add(address);
					 //System.out.println(in.getKey().getText()+"   "+in.getValue());
				 }
			}
		}
	}
	
	public void parseOutAddr(Map.Entry<JsonStringNode, JsonNode> entry){
		List<JsonNode> elements = entry.getValue().getElements();
		for(JsonNode element : elements){
			List<JsonField> el = element.getFieldList();
			for(JsonField e : el){
				Address address = new Address();
				 address.setOutput(true);
				 address.createAddrFromJson(e.getName().getText(), e.getValue().getText());
				 this.outAddress.add(address);
			}
		}
	}
	
	public void showInfo(){
		System.out.println("**************");
		System.out.println("Transaction:");
		System.out.println("Hash: "+hash);
		System.out.println("Included In Blocks: "+blockHeight+" Received Time: ");
		System.out.println("Relayed by IP: "+ip+" Input Addresses: "+inNum+" Output Addresses: "+outNum);
		System.out.println("**************");
	}
	
	public void showMore(){
		System.out.println(txs.size()+" Transactions requested");
	}

}
