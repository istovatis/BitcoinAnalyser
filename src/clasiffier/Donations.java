package clasiffier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;

import org.eclipse.jetty.util.MultiMap;
import org.junit.Test;

import parser.WebPageParser;


import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlDivision;
import com.gargoylesoftware.htmlunit.html.HtmlImage;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.html.HtmlTable;
import com.gargoylesoftware.htmlunit.html.HtmlTableCell;
import com.gargoylesoftware.htmlunit.html.HtmlTableRow;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import database.DBConnection;

/**
 * This is a blockChain.info parser. The aim of this parser is to get address,
 * tag and link of every tagged public key and finally the verification status
 * of this address. This class makes use of htmlUnit library.
 * 
 * @author istovatis
 * 
 */
public class Donations {

	private static HashSet<Donations> tagList = new HashSet<Donations>();
	Multimap<String, String> mostTaged = ArrayListMultimap.create();

	private final String blockChain = "https://blockchain.info/tags?offset=";	// the webpage
	private String xpath = "//table[@class='table table-striped']";
	private String address;		
	private String tag;
	private String link;
	private boolean verified;

	private int numVerified;	//Number of verified tags
	private int numNotVerified;		//Number of not verified tags
	private final static int offset = 200;
	
	public void parserForDonations() throws Exception {
		System.out.println("Starting finding donations from blockchain at "+new Date());
		for (int range =0 ; range< 12; range++ ) {
			List<?> list = WebPageParser.parseWebPage(blockChain+offset*range, xpath);
			if (list.size() > 0) {
				HtmlTable table = (HtmlTable) list.get(0);
				for (int i = 1; i < table.getRowCount(); i++) {
					final HtmlTableRow row = table.getRow(i);
					int rowCell = 0;
					Donations record = new Donations();
					for (final HtmlTableCell cell : row.getCells()) {
						if (rowCell == 0) {		//get the address
							record.address = cell.asText();
						} else if (rowCell == 1) {		//get the tag
							record.tag = cell.asText();
						} else if (rowCell == 2) {		// get the link
							record.link = cell.asText();
						} else if (rowCell == 3) {
							if (cell.asText().equals("")) {
								String text = cell.asXml();
								if (text.length() > 1) {
									int start = text.indexOf("src=");
									int end = text.indexOf("png");
									if (start > 1 && end > 1) {		//get the verification status
										String verified = text.substring(start,
												end - 1);
										if (verified.endsWith("green_tick")) {
											record.verified = true;
											numVerified++;
											// System.out.println("Verified!");
										} else if (verified.endsWith("red_cross")) {
											// System.out.println("Not verified!");
											record.verified = false;
											numNotVerified++;
										}
									}
								} else {
									System.out.println(record.link
											+ " Not classified");
								}
							}
							mostTaged.put(record.link, record.address);
							tagList.add(record);
						}
						// System.out.println("   Found cell: " + cell.asText());
						rowCell++;
					}
				}
			} else {
				System.out.println("Tag list not parsed from website");
			}
		}
		//showAllRecords();
		insertToDB();
		showStats();
	}

	public void showStats() {
		int numTags = tagList.size();
		System.out.println("Number of tags: " + numTags);
		System.out.println("Number of verified tags: " + numVerified);
		System.out.println("Number of Not verified tags: " + numNotVerified);
		System.out.println(Math.abs((numTags - (numVerified + numNotVerified)))
				+ " addresses not classified");
	}

	public void showAllRecords() {
		for (Donations record : tagList) {
			System.out.println(record.address + " " + record.tag + " "
					+ record.link + " " + record.verified);
		}
	}

	/**
	 * insert parsed tags to database. Table:tags, columns: addreess, tag, link,
	 * verified.
	 */
	public void insertToDB() {
		String insertTableSQL = "INSERT INTO donation"
				+ "(address, tag, link, verified) VALUES" + "(?,?,?,?)";
		Connection connection = DBConnection.get().connectPostgre();
		try {
			PreparedStatement preparedStatement = connection
					.prepareStatement(insertTableSQL);
			for (Donations record : tagList) {
				System.out.println(record.address);
				preparedStatement.setString(1, record.address);
				preparedStatement.setString(2, record.tag);
				preparedStatement.setString(3, record.link);
				preparedStatement.setBoolean(4, record.verified);

				preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
