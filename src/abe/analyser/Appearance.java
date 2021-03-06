package abe.analyser;

import java.sql.ResultSet;
import java.sql.SQLException;
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
		for (int pk = 0; pk< maxTx; pk++) {
			String select = "SELECT a.tx_id as ins, b.tx_id as outs FROM txout b LEFT JOIN txin a ON a.txout_id = b.txout_id  WHERE pubkey_id = " + pk;
			try {
				preparedStatement = connection.prepareStatement(select);
				ResultSet rs = preparedStatement.executeQuery();
				insAndOuts = new HashSet();
				while (rs.next()) {
					int ins = rs.getInt(1);
					int outs = rs.getInt(2);
					if(ins != 0)
						insAndOuts.add(ins);
					if(outs != 0)
						insAndOuts.add(outs);
				}
				pubKeyToFound.put(pk, insAndOuts.size());
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
	}
	
	public void fixPkFound() {
		for (Map.Entry<Integer, Integer> entry : pubKeyToFound.entrySet()) {
			Integer key = entry.getKey();
			Integer value = entry.getValue();
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
