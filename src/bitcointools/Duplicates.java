package bitcointools;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

import database.DBInteraction;
import database.Queries;

import argo.jdom.JsonNode;
import argo.jdom.JsonStringNode;

public class Duplicates extends DBInteraction {
	private final String file = "my_data/new_generated";
	String selectTableSQL = Queries.duplicateTxHashes();

	@Override
	public void readDataFile() {
		try {
			br = new BufferedReader(new InputStreamReader(new DataInputStream(
					new FileInputStream(DBInteraction.path + file))));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			preparedStatement = connection.prepareStatement(selectTableSQL);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String strLine;
		int rows = 0;
		try {
			while ((strLine = br.readLine()) != null) {
				if (rows<=0) {
					try {
						int sleep = Scraper.getRandom();
						Thread.sleep(sleep);
					} catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
					Scraper scraper = new Scraper();
					scraper.excutePost(
							"http://blockchain.info/rawtx/" + strLine
									+ "?format=json", "");
					scraper.parseJson();
					Transaction tx = new Transaction();
 					for (Map.Entry<JsonStringNode, JsonNode> entry : scraper.getMap().entrySet()) {
						tx.createTxFromJson(entry);
					}
 					if(tx.isHasTagedAddress()){
 						tx.getTaggedTxs().add(tx.getHash());
 					}
 					Transaction.txs.add(tx);
					//tx.showMore();
					rows++;
				}
				else
				{
					rows++;
				}
					if(rows % 1000 == 0){
						//Address.showTagStats();
					}
			}
			System.out.println(Address.getGenericCount()+" counter");
			Address.showHashTags();
			Address.showTagStats();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
