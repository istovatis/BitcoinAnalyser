package bitcointools;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import database.DBInteraction;

import argo.jdom.JsonNode;
import argo.jdom.JsonStringNode;

public class Transactions extends DBInteraction {
	private final String file = "query_results/test";
	private final static int totalParsed = 1000;
	private static int NTHREDS = 2;
	private static ArrayList<Transaction> txList = new ArrayList<Transaction>();
	private static ArrayList<Transaction> parsedTxList = new ArrayList<Transaction>();
	private int current;
	private int rows; // number of text rows
	HashSet<Integer> hashSet;
	private AtomicInteger value = new AtomicInteger();
	BufferedWriter errorBw;
	BufferedWriter bw;

	public int getCurrent() { return current; }
	public void setCurrent(int current) { this.current = current; }

	public int getRows() {
		return rows;
	}

	public int getValue() {
		return value.get();
	}

	public int increment() {
		return value.incrementAndGet();
	}

	public Transaction getTx(int pos) {
		return txList.get(pos % txList.size());
	}

	public int getListSize() {
		return txList.size();
	}

	public void readDataFile(String file) {
		txList.clear();
		try {
			br = new BufferedReader(new InputStreamReader(new DataInputStream(
					new FileInputStream(file))));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		String strLine;
		try {
			Date date = new Date();
			System.out.println("Parsing " + totalParsed + " txs starting at "
					+ date);
			while ((strLine = br.readLine()) != null) {
				String[] splits = strLine.split(";");
				Integer id = Integer.valueOf(splits[0]);
				String hash = splits[1]; // edit to hash
				Transaction tx = new Transaction(id, hash);
				// tx.showMore();
				txList.add(tx);
				rows++;
				if (rows % 500000 == 0) {
					Date date2 = new Date();
					System.out.println("Parsed " + rows + " txs starting at "
							+ date2);
				}
			}
			Date date2 = new Date();
			System.out.println("Parsing " + rows + " txs finished at" + date2);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void requestBlockchain(Transaction tx) {
		Scraper scraper = new Scraper();

		String result = scraper
				.excutePost("http://blockchain.info/rawtx/" + tx.getHash()
						+ "?format=json", "");
		if (result.equals("error")) {
			if(tx.getNumRequests()< 8){
				//System.out.println("Try again "+tx.getId()+ " "+tx.getNumRequests());
				tx.oneMoreRequest();
				requestBlockchain(tx);
			}
			else{
				try {
					//System.out.println("error!");
					errorBw.write(tx.getId() + "," + tx.getHash());
					errorBw.newLine();
					// errorBw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}		
		} else {
			scraper.parseJson();
			for (Map.Entry<JsonStringNode, JsonNode> entry : scraper.getMap()
					.entrySet()) {
				tx.createTxFromJson(entry);
			}
		}
	}

	public BufferedWriter initErrorFileWriter() {
		String outF = File.separator + "query_results" + File.separator
				+ "not_parsed_txs";
		File outFile = new File(path + outF);
		// if file doesnt exists, then create it
		try {
			if (!outFile.exists()) {
				outFile.createNewFile();
			}
			FileWriter fw = new FileWriter(outFile.getAbsoluteFile());
			errorBw = new BufferedWriter(fw);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return errorBw;
	}

	public BufferedWriter initFileWriter() {
		String outF = File.separator + "query_results" + File.separator
				+ "parsed_txs";
		File outFile = new File(path + outF);
		// if file doesnt exists, then create it
		try {
			if (!outFile.exists()) {
				outFile.createNewFile();
			}
			FileWriter fw = new FileWriter(outFile.getAbsoluteFile());
			bw = new BufferedWriter(fw);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return bw;
	}

	public void closeErrorBf() {
		try {
			errorBw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void addParsedTxToFile() {
		bw = initFileWriter();
		for (Transaction tx : parsedTxList) {
			if (tx.getNumAddresses() > 0) {
				String inAddresses = "{";
				for (Address ad : tx.getInAddresses()) {
					inAddresses = inAddresses + ad.getAddress();
				}
				inAddresses = inAddresses + "}";
				String outAddresses = "{";
				for (Address ad : tx.getOutAddresses()) {
					//System.out.println(ad.getAddress() + "_");
					outAddresses = outAddresses + ad.getAddress() + ",";
				}
				outAddresses = outAddresses + "}";
				String content = tx.getId() + "," + tx.getHash() + ","
						+ inAddresses + "," + outAddresses;
				//System.out.println(content);
				try {
					bw.write(content);
					bw.newLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		try {
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		String inPath = File.separator + "query_results" + File.separator;
		final Transactions counter = new Transactions();
		List<Future<Integer>> list = new ArrayList<Future<Integer>>();
		List<Transaction> tList = new ArrayList<Transaction>();

		// check the number of available processors
		NTHREDS = Runtime.getRuntime().availableProcessors();
		System.out.println(NTHREDS + " available processors");
		ExecutorService executor = Executors.newFixedThreadPool(NTHREDS);
		int startFile = 0;
		int endFile = 3;
		counter.initErrorFileWriter();
		String file = path + inPath + "input"+0;
		counter.readDataFile(file);
		System.out.println(" tx list size: " + counter.getListSize());
		Date date = new Date();
		System.out.println("Starting blockchain request at " + date);
		counter.hashSet = new HashSet<Integer>();

		for (int j = 0; j < counter.txList.size(); j++) {
			// while(counter.hashSet.size()< 16){
			Callable<Integer> worker = new Callable<Integer>() {
				@Override
				public Integer call() throws Exception {
					Transaction tx = counter.getTx(counter.getValue());
					counter.requestBlockchain(tx);
					int number = counter.increment();
					if(number % 10000==0){
						Date d = new Date();
						System.out.println("Requested "+number+" from blockchain at "+d);
						System.out.println("Dammed requests "+tx.getDummedRequests());
					}
					parsedTxList.add(tx);
					counter.hashSet.add(Integer.valueOf(number));
					return number;
				}
			};
			Future<Integer> submit = executor.submit(worker);
			list.add(submit);

		}

		// This will make the executor accept no new threads
		// and finish all existing threads in the queue
		executor.shutdown();
		// Wait until all threads are finish
		while (!executor.isTerminated()) {
		}
		if (executor.isTerminated()) {
			Date date2 = new Date();
			System.out.println("Finished blockchain request at" + date2);
			counter.addParsedTxToFile();
			counter.closeErrorBf();
		}
		Set<Integer> set = new HashSet<Integer>();
		for (Future<Integer> future : list) {
			try {
				set.add(future.get());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		if (list.size() != set.size()) {
			throw new RuntimeException("Double-entries!!!");
		}
	}
}
