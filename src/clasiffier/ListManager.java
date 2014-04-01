package clasiffier;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import clasiffier.ListAddress.Cluster;

import bitcointools.Config;
import bitcointools.Database;
import bitcointools.HasParser;
import bitcointools.Key;

public class ListManager extends Key {
	private static List<ListAddress> listAddresses;
	protected String file;
	protected String column;
	String path = HasParser.path + "/Lists/";

	public ListManager(Cluster cluster) {
		listAddresses = new ArrayList<ListAddress>();
		setFile(cluster.name());
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = path + file + ".csv";
	}

	public void setTable(String table) {
		this.table = table;
	}

	public ListManager() {
		listAddresses = new ArrayList<ListAddress>();
	}

	public static List<ListAddress> getListAddresses() {
		return listAddresses;
	}

	public void register(ListAddress listAddress) {
		listAddresses.add(listAddress);
	}

	public void readDataFile() {
		//
	}
	
	/**
	 * Read .csv file. Then for every pubkey_hash get its pubkey_id
	 * @param cluster
	 */
	public void generate(Enum cluster) {
		try {
			connection = Database.get().connectPostgre();
			br = new BufferedReader(new InputStreamReader(new DataInputStream(new FileInputStream(file))));
			String getIdSql = "SELECT pubkey_id from pubkey WHERE pubkey_hash = ?";
			preparedStatement = connection.prepareStatement(getIdSql);
			String strLine;
			int rows = 0;
			ListAddress listAddress;
			while ((strLine = br.readLine()) != null) {
				String[] line = strLine.split(",");
				String address = line[0];
				listAddress = new ListAddress(address, (Cluster) cluster);
				preparedStatement.setString(1, listAddress.getAddress());

				ResultSet rs = preparedStatement.executeQuery();
				while (rs.next()) {
					listAddress.setId(rs.getInt(1));
				}
				register(listAddress);
				rows++;
			}
			System.out.println(rows + " addresses added at " + cluster.name() + " Manager");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}
}
