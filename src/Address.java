import java.util.Map;

import argo.jdom.JsonNode;
import argo.jdom.JsonStringNode;


public class Address {
	private int n;
	private String address;
	private double value;
	private int tx_index;
	private int type;
	private boolean inInput;	// This address is input in a transaction
	private boolean output;	// This address is output in a transaction
	private String addr_tag;	// Some addresses have a tag
	
	public boolean isOutput() {
		return output;
	}

	public void setOutput(boolean output) {
		this.output = output;
	}

	public boolean isInInput() {
		return inInput;
	}

	public void setInInput(boolean inInput) {
		this.inInput = inInput;
	}

	public void createAddrFromJson(String name, String value) {
		if(name.equals("n")){
			this.n = Integer.valueOf(value);
		}
		else if(name.equals("value")){
			this.value = Double.valueOf(value);
		}
		else if(name.equals("addr")){
			this.address = value;
		}
		else if(name.equals("tx_index")){
			this.tx_index = Integer.valueOf(value);
		}
		else if(name.equals("type")){
			this.type = Integer.valueOf(value);
		}
	}
}
