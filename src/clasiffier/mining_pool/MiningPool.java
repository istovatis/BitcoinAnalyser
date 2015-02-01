package clasiffier.mining_pool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import org.junit.Test;


import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.html.HtmlTable;
import com.gargoylesoftware.htmlunit.html.HtmlTableCell;
import com.gargoylesoftware.htmlunit.html.HtmlTableRow;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import database.DBConnection;

/**
 * This is a Bitcoin Block Origin - Block List parser. The aim of this parser is to get id,
 * block_hash and mining_name of every block discovered by mining pools
 * 
 * This class makes use of htmlUnit library.
 * 
 * @author istovatis
 * 
 */
public class MiningPool {

	private static HashSet<MiningPool> tagList = new HashSet<MiningPool>();
	Multimap<String, String> mostTaged = ArrayListMultimap.create();

	private final String blockChain = "http://blockorigin.pfoe.be/blocklist.php";	// the webpage
	protected static Connection connection = DBConnection.get().connectPostgre();
	protected static PreparedStatement preparedStatement;
	private String id;		
	private Integer block_id;
	private String block_hash;
	private String mining_name;
	
	@Test
	public void homePage() throws Exception {
		System.out.println("Starting finding blocks discovered by mining pools at "+new Date());
		final WebClient webClient = new WebClient();
		webClient.getOptions().setCssEnabled(false);
		webClient.getOptions().setJavaScriptEnabled(false);
			final HtmlPage page = webClient.getPage(blockChain);
			List<?> list = page.getByXPath("//table[@class='nice']");
			if (list.size() > 0) {
				HtmlTable table = (HtmlTable) list.get(0);
				for (int i = 1; i < table.getRowCount(); i++) {
					final HtmlTableRow row = table.getRow(i);
					int rowCell = 0;
					MiningPool record = new MiningPool();
					for (final HtmlTableCell cell : row.getCells()) {
						if (rowCell == 0) {		
							record.id = cell.asText();
						} else if (rowCell == 1) {		
							record.block_hash = cell.asText();
						} else if (rowCell == 2) {		
							record.mining_name = cell.asText();
						} else if (rowCell == 3) {
							if (cell.asText().equals("")) {
								String text = cell.asXml();
							}
							mostTaged.put(record.mining_name, record.id);
							record.block_id = getBlockId(record.block_hash);
							tagList.add(record);
						}
						// System.out.println("   Found cell: " + cell.asText());
						rowCell++;
					}
				}
			} else {
				System.out.println("Tag list not parsed from website");
			}
		
		 showAllRecords();
		insertToDB();
		showStats();
		webClient.closeAllWindows();
	}

	public void showStats() {
		int numTags = tagList.size();
		System.out.println("Number of blocks: " + numTags);
	}

	public void showAllRecords() {
		for (MiningPool record : tagList) {
			System.out.println(record.id + " " + record.block_hash + " "
					+ record.mining_name);
		}
	}

	/**
	 * insert parsed tags to database. Table:tags, columns: addreess, tag, link,
	 * verified.
	 */
	public void insertToDB() throws SQLException{
		String insertTableSQL = "INSERT INTO mining_pool"
				+ "(id, block_id, block_hash, pool_name) VALUES" + "(?,?,?,?)";
		try {
			preparedStatement = connection
					.prepareStatement(insertTableSQL);
			for (MiningPool record : tagList) {
				System.out.println(record.id);
				preparedStatement.setInt(1, Integer.valueOf(record.id));
				preparedStatement.setInt(2, record.block_id);
				preparedStatement.setString(3, record.block_hash);
				preparedStatement.setString(4, record.mining_name);

				preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getNextException());
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
	}
	
	public Integer getBlockId(String block_hash) {
		String getBlockId = "Select block_id From Block where block_hash = '" + block_hash + "'";
		try {
			preparedStatement = connection.prepareStatement(getBlockId);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				return rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getNextException());
		}
		return 0;
	}
}
