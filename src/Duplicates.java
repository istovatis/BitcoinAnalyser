import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Random;

public class Duplicates extends HasParser {
	private final String file = "my_data/new_duplicate_trans";
	String selectTableSQL = Queries.duplicateTxHashes();

	@Override
	public void readDataFile() {
		try {
			br = new BufferedReader(new InputStreamReader(new DataInputStream(
					new FileInputStream(HasParser.path + file))));
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
		Transaction tx;
		try {
			while ((strLine = br.readLine()) != null) {
				if (rows < 2) {
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
					System.out.println(strLine);
					System.out.println(scraper.excutePost(
							"http://blockchain.info/rawtx/" + strLine
									+ "?format=json", ""));
					scraper.parseJson();
					tx = new Transaction();
					tx.createTxFromJson(scraper.getMap());
					//tx.showMore();
					rows++;
				}
				else
				{
					rows++;
				}
					if(rows % 10000 == 0){
						Address.showTagStats();
					}
			}
			Address.showTagStats();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
