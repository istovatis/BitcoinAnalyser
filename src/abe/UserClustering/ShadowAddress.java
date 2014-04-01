package abe.UserClustering;
import java.util.ArrayList;


public class ShadowAddress {
	private int id;
	private int txId;
	private int txoutId;
	private int pubKey;	
	public static ArrayList<ShadowAddress> shadowAddresses = new ArrayList<ShadowAddress>();

	public ShadowAddress(int txId, int txoutId, int pubKey){
		this.txId = txId;
		this.txoutId = txoutId;
		this.pubKey = pubKey;
		id += 1;
	}
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getTxId() {
		return txId;
	}

	public void setTxId(int txId) {
		this.txId = txId;
	}

	public int getTxoutId() {
		return txoutId;
	}

	public void setTxoutId(int txoutId) {
		this.txoutId = txoutId;
	}

	public int getPubKey() {
		return pubKey;
	}

	public void setPubKey(int pubKey) {
		this.pubKey = pubKey;
	}
}
