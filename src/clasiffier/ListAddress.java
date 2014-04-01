package clasiffier;

public class ListAddress {
	private String address;
	private String name;
	private int id; // The id of address as stored at pubkey table
	public static final String bitcointalk = "bitcointalk";
	private  final Cluster cluster;
	
	public enum Cluster {
		bitcointalk, casascius, bitcoinotc, donations, exchanges, gambling, malware, mining, shareholders, silkroad
	}
	
	public ListAddress (String address, Cluster cluster) {
		this.address = address;
		this.cluster = cluster;
	}
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}
}
