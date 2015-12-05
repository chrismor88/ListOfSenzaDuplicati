
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;



public class Main {

	final static String PATH_FILE_LIST_OF = "/home/chris88/Documenti/Tesi/wiki_tables/list_of/List_of_2_relevants_columns_filtered.txt";

	int counterCompletedTables;

	public Main() {
		counterCompletedTables = 0;
	}


	public synchronized void incrementCounter(){
		counterCompletedTables++;
	}

	public synchronized int getCounter(){
		return counterCompletedTables;
	}


	public static void main(String[] args) {
		executionForListOf();

	}


	private static void executionForListOf(){
		Map<String,Pair<Integer,Integer>> title_nrows_nline = new HashMap<String,Pair<Integer,Integer>>();
		Main main = new Main();

		long startInMillis = 0, endInMillist = 0, totalInMillis = 0;

		Calendar calendar = Calendar.getInstance();
		java.util.Date today = calendar.getTime();

		File fileListOf = new File(PATH_FILE_LIST_OF);


		Producer producer = null;

		try {


			BlockingQueue<String> messageBuffer = new LinkedBlockingQueue<String>(1000);
			BlockingQueue<String> responseBuffer = new LinkedBlockingQueue<String>(10);

			producer = new Producer(fileListOf, messageBuffer, responseBuffer);

			producer.start();
			Thread.sleep(2000);

			startInMillis = System.currentTimeMillis();

			int cores = 1;
			Consumer[] consumers = new Consumer[cores];


			for(int i=0; i<cores;i++){
				consumers[i] = new Consumer(messageBuffer, responseBuffer,title_nrows_nline,main);
				consumers[i].start();
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		try {
			producer.join();
			
			scanFileListOf(fileListOf,title_nrows_nline);
			

			System.out.println("Finished");
			endInMillist = System.currentTimeMillis();
			totalInMillis = endInMillist - startInMillis;
			System.out.println("Total time in millis: "+totalInMillis);
			double totalTimeInSeconds = (double)totalInMillis /1000;
			System.out.println("Total time in seconds: "+totalTimeInSeconds);
		}catch (InterruptedException e) {
			e.printStackTrace();
		} 

	}


	private static void scanFileListOf(File fileListOf, Map<String, Pair<Integer,Integer>> title_nrows_nline) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(fileListOf));
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File("/home/chris88/Documenti/Tesi/wiki_tables/list_of/List_of_senza_duplicati.txt")));
			Collection<Pair<Integer,Integer>> pairs = title_nrows_nline.values();
			List<Integer> numbersOfLine = new ArrayList<Integer>();
			
			for(Pair<Integer,Integer> p : pairs){
				numbersOfLine.add(p.getSecond());
			}
			
			
			String line = "";
			int counter = 1;
			while((line=reader.readLine())!=null){
				if(numbersOfLine.contains(counter))
					writer.append(line+"\n");
			counter++;
			}
			writer.flush();
			writer.close();
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		
	}

}
