
package abe.analyser;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import database.DBDataReceiver;
import database.DBDataUpdater;

public class Appearance extends DBDataReceiver {
	
	private Map<Integer, Integer> pubKeyToFound = new HashMap<Integer, Integer>();
	private HashSet<Integer> insAndOuts;

	/**
	 * For every public key find where this pubkey was involved as an input or output.
	 */
	public void pkAppearFound() {
		findBounds("pubkey", "pubkey_id");
		int pk = 0;
		int newPk = 0;
		String select = "SELECT a.tx_id as ins, b.tx_id as outs, b.pubkey_id  FROM txout b LEFT JOIN txin a ON a.txout_id = b.txout_id WHERE pubkey_id > 10000000 and pubkey_id <= " + maxTx + " order by pubkey_id ";
		try {
			preparedStatement = connection.prepareStatement(select);
			ResultSet rs = preparedStatement.executeQuery();
			insAndOuts = new HashSet();
			while (rs.next()) {
				newPk = rs.getInt(3);
				if (pk != newPk) {
					pubKeyToFound.put(pk, insAndOuts.size());
					insAndOuts = new HashSet();
					pk = newPk;
					if (pk % 1000000 == 0)
						System.out.println(pk + " public keys scanned at " + new Date());
				}
				int ins = rs.getInt(1);
				int outs = rs.getInt(2);
				if(ins != 0)
					insAndOuts.add(ins);
				if(outs != 0)
					insAndOuts.add(outs);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getNextException());
		}
		finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}	
	}
	
	public void fixPkFound() {
		System.out.println(" Starting fixing found field at " + new Date());
		for (Map.Entry<Integer, Integer> entry : pubKeyToFound.entrySet()) {
			Integer key = entry.getKey();
			Integer value = entry.getValue();
			if (key % 1000000 == 0)
				System.out.println(key + " found field updates at " + new Date());
			DBDataUpdater.updateInteger("pk_appear", "found", value, "pubkey_id = " + key);
		}
	}
	
	public  void start() {
		pkAppearFound();
		fixPkFound();
	}

	@Override
	public void readDataFile() {
		// TODO Auto-generated method stub
		
	}
}