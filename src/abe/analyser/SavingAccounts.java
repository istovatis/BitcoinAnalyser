package abe.analyser;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import database.DBDataReceiver;
import database.DBInteraction;

/**
 * Saving Accounts are defined as those public keys that receive BTCs and store them without sending any amount.
 * The Accounts Added are those accounts that are added within a time period. On the contrary, Accounts Removed
 * are the prior saving accounts that within the latest time period were reported as senders in any transaction.
 */
public class SavingAccounts extends DBInteraction {

	private static final int daysParsed = 1648;//1590;
	private static HashSet<Integer> senders = new HashSet<Integer>();
	private static HashSet<Integer> savingAccounts = new HashSet<Integer>();
	
	private int startOfDay = 1231006505; 
	private int endOfDay;
	private int firstTx;
	private int lastTx;
	private int firstBlock;
	private int lastBlock;
	private int accountsAdded;
	private int accountsRemoved;
	private int priorSenders;
	
	FileWriter fw;
	BufferedWriter bw;
	
	FileWriter fwAdded;
	BufferedWriter bwAdded;
	
	FileWriter fwRemoved;
	BufferedWriter bwRemoved;

	FileWriter fwSenders;
	BufferedWriter bwSenders;
	
	public void setEndOfDay() {
		endOfDay = startOfDay + 86400;
	}
	
	public int getEndOfDay() { return endOfDay; }
	
	public void setStartOfDay(int startOfDay) { this.startOfDay = startOfDay; }
	
	public SavingAccounts() {
		String outF = File.separator + "query_results" + File.separator + "saving_accounts_all";
		File outFile = new File(path + outF);
		
		String outFAdded = File.separator + "query_results" + File.separator + "saving_accounts_added_all";
		File outFileAdded = new File(path + outFAdded);
		
		String outFRemoved = File.separator + "query_results" + File.separator + "saving_accounts_removed_all";
		File outFileRemoved = new File(path + outFRemoved);
		
		String outFsenders = File.separator + "query_results" + File.separator + "new_senders";
		File outFileSenders = new File(path + outFsenders);

		
		try {
			fw = new FileWriter(outFile.getAbsoluteFile());
			fwAdded = new FileWriter(outFileAdded.getAbsoluteFile());
			fwRemoved = new FileWriter(outFileRemoved.getAbsoluteFile());
			fwSenders = new FileWriter(outFileSenders.getAbsoluteFile());
		} catch (IOException e) {
			e.printStackTrace();
		}
		bw = new BufferedWriter(fw);
		bwAdded = new BufferedWriter(fwAdded);
		bwRemoved = new BufferedWriter(fwRemoved);
		bwSenders = new BufferedWriter(fwSenders);
	}

	@Override
	public void readDataFile() { }
	
	public void findBlocksOfTheDay() {
		String query = "SELECT MIN(block_id), MAX(block_id) "
				+ " FROM block "
				+ " WHERE block.block_ntime BETWEEN " + startOfDay + " AND " + endOfDay;
		List<Integer> blocksOfDay = (List<Integer>) DBDataReceiver.selectTwoFieldsQuery(query);
			firstBlock = blocksOfDay.get(0);
			lastBlock = blocksOfDay.get(1);
	}
	
	public void findTxsOfTheDay() {
		String query = "SELECT distinct(tx_id) "
				+ " FROM block_tx "
				+ " WHERE block_id BETWEEN " + firstBlock + " AND " + lastBlock
				+ " ORDER BY tx_id  ";
		List<Integer> txsOfDay = (List<Integer>) DBDataReceiver.selectQuery(query);
		if(!txsOfDay.isEmpty()) {
			firstTx= txsOfDay.get(0);
			lastTx= txsOfDay.get(txsOfDay.size() - 1);		
		}
	}
	
	/**
	 * Find senders of the day
	 */
	public void findTxinsOfTheDay() {
		String query = "select distinct(pubkey_id) from txout where txout_id in (select distinct(txout_id) from txin where tx_id between " + firstTx + " and " + lastTx + ")";
		List<Integer> txinsOfDay = (List<Integer>) DBDataReceiver.selectQuery(query);
		priorSenders = senders.size();
		senders.addAll(txinsOfDay);
		removeSavingAccount(txinsOfDay);
	}
	
	/**
	 * Find receivers of the day
	 */
	public void findTxoutsOfTheDay() {
		String query = "select distinct(pubkey_id) from txout where tx_id between " + firstTx + " and " + lastTx;
		List<Integer> txoutsOfDay = (List<Integer>) DBDataReceiver.selectQuery(query);
		addSavingAccount(getOnlyReceivers(txoutsOfDay));
	}
	
	private List<Integer> getOnlyReceivers(List<Integer> receivers) {
		//HashSet<Integer> tmpSender = senders;
		int size = receivers.size();
		receivers.removeAll(senders);
		System.out.println("Total senders: " + senders.size() + ". Prioir sending receivers " + (size - receivers.size()));
		return receivers;
	}
	
	public void addSavingAccount(List<Integer> txinsOfDay) {
		int size1 = savingAccounts.size();
		savingAccounts.addAll(txinsOfDay);
		accountsAdded = savingAccounts.size() - size1;
		System.out.print("Added " + accountsAdded + " to savings -- ");
	}
	
	public void removeSavingAccount(List<Integer> txoutsOfDay) {
		int size1 = savingAccounts.size();
		savingAccounts.removeAll(txoutsOfDay);
		accountsRemoved  = size1 - savingAccounts.size();
		System.out.println("Removed " + accountsRemoved + " from savings" + "Total savings: " + savingAccounts.size());
	}

	public void writeToFile() {
		try {
			int numOfAccounts = savingAccounts.size();
			bw.write(String.valueOf(numOfAccounts));
			bw.newLine();
			
			bwAdded.write(String.valueOf(accountsAdded));
			bwAdded.newLine();
			
			bwRemoved.write(String.valueOf(accountsRemoved));
			bwRemoved.newLine();
			
			int newSenders = senders.size() - priorSenders;
			bwSenders.write(String.valueOf(newSenders));
			bwSenders.newLine();
			//bw.close();
			System.out.println("Parsed  " + (lastBlock - firstBlock) + " blocks at " + new Date() + ". Found " + numOfAccounts
					+ " saving accounts " + firstTx + "-" + lastTx + " **" + (endOfDay-startOfDay) + " StartOfDay:" + startOfDay);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void start() {
		SavingAccounts accounts = new SavingAccounts();
		System.out.println("finding saving accounts. Start at " + new Date());
		try {
			for (int i = 0; i < daysParsed; ++i) {
				accounts.setEndOfDay();
				accounts.findBlocksOfTheDay();
				accounts.findTxsOfTheDay();
				accounts.findTxinsOfTheDay();
				accounts.findTxoutsOfTheDay();
				accounts.writeToFile();
				accounts.setStartOfDay(accounts.getEndOfDay());
				System.out.println(i + ":");
		}
		
			accounts.bw.close();
			accounts.bwAdded.close();
			accounts.bwRemoved.close();
		} catch (Exception e) {
			try {
				accounts.writeToFile();
				accounts.bw.close();
				accounts.bwAdded.close();
				accounts.bwRemoved.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
}
