package clasiffier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ListAddress {
	private String address;
	private String name;
	private String value;

	private int id; // The id of address as stored at pubkey table
	public static final String bitcointalk = "bitcointalk";
	private  final Cluster cluster;
	private static List<String> complexClusters = new ArrayList<String>(Arrays.asList("exchange"));
	
	private static boolean complexCluster;
	
	public enum Cluster {
		bitcointalk, casascius, bitcoinotc, donations, exchanges, gambling, malware, mining, shareholders, silkroad
	}
	
	public ListAddress (String address, Cluster cluster) {
		this.address = address;
		this.cluster = cluster;
	}
	
	public ListAddress (String address, String value, Cluster cluster) {
		this.address = address;
		this.cluster = cluster;
		this.value = value;
	}
	
	public static void initComplexClusters() {
		complexClusters.add("exchange");
	}
	
	public static void setIsComplexClusters(String cluster) {
		if (complexClusters.contains(cluster))
			complexCluster = true;
		else
			complexCluster = false;
	}
	
	public static boolean isComplexCluster(){
		return complexCluster;
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
	
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
