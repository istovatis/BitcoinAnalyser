package abe.analyser;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import database.DBDataReceiver;
import database.DBInteraction;

public class SavingAccounts extends DBInteraction {

	private static final int daysParsed = 1648;
	private static HashSet<Integer> receivers = new HashSet<Integer>();
	private static HashSet<Integer> savingAccounts = new HashSet<Integer>();
	
	private int startOfDay = 1231006505; 
	private int endOfDay;
	private int firstTx;
	private int lastTx;
	private int firstBlock;
	private int lastBlock;
	
	FileWriter fw;
	BufferedWriter bw;

	public void setEndOfDay() {
		endOfDay = startOfDay + 86400;
	}
	
	public int getEndOfDay() {
		return endOfDay;
	}
	
	public void setStartOfDay(int startOfDay) {
		this.startOfDay = startOfDay;
	}
	
	public SavingAccounts() {
		String outF = File.separator + "query_results" + File.separator + "saving_accounts";
		File outFile = new File(path + outF);
		try {
			fw = new FileWriter(outFile.getAbsoluteFile());
		} catch (IOException e) {
			e.printStackTrace();
		}
		bw = new BufferedWriter(fw);
	}

	@Override
	public void readDataFile() {

	}

	
	public void findBlocksOfTheDay() {
		String query = "select min(block_id), max(block_id) from block where block.block_ntime between " + startOfDay + " and  " + endOfDay;
		List<Integer> blocksOfDay = (List<Integer>) DBDataReceiver.selectTwoFieldsQuery(query);
			firstBlock = blocksOfDay.get(0);
			lastBlock = blocksOfDay.get(1);
	}
	
	public void findTxsOfTheDay() {
		String query = "select distinct(tx_id) from block_tx where block_id between " + firstBlock + " and " + lastBlock + " order by tx_id  ";
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
		String query = "select txout_id from txin where tx_id between " + firstTx + " and " + lastTx;
		List<Integer> txinsOfDay = (List<Integer>) DBDataReceiver.selectQuery(query);
		receivers.addAll(txinsOfDay);
		removeSavingAccount(txinsOfDay);
	}
	
	/**
	 * Find receivers of the day
	 */
	public void findTxoutsOfTheDay() {
		String query = "select txout_id from txout where tx_id between " + firstTx + " and " + lastTx;
		List<Integer> txoutsOfDay = (List<Integer>) DBDataReceiver.selectQuery(query);
		addSavingAccount(txoutsOfDay);
	}
	
	public void addSavingAccount(List<Integer> txinsOfDay) {
		int size1 = savingAccounts.size();
		savingAccounts.addAll(txinsOfDay);
		for (Integer txin : txinsOfDay) {
			if (!receivers.contains(txin))
				savingAccounts.add(txin);
		}
		System.out.print("Added " + ( savingAccounts.size() - size1) + " to savings -- ");
	}
	
	public void removeSavingAccount(List<Integer> txoutsOfDay) {
		int size1 = savingAccounts.size();
		savingAccounts.removeAll(txoutsOfDay);
		System.out.println("Removed " + (size1 - savingAccounts.size()) + " from savings");
	}

	public void writeToFile() {
		try {
			int numOfAccounts = savingAccounts.size();
			bw.write(String.valueOf(numOfAccounts));
			bw.newLine();
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
		}
		
			accounts.bw.close();
		} catch (IOException e) {
			try {
				accounts.bw.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
}
