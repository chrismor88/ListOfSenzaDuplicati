package goldenset1000;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Main {


	private static final String STATISTICAL_FILE = "/home/chris88/Documenti/Tesi/wiki_tables/list_of/11K_hard_selection_list_of_tables_statistics_2_relevant_columns.tsv";
	private static final String GOLDEN_SET_DUMP = "/home/chris88/Documenti/Tesi/wiki_tables/list_of/Golden_set_1100.txt";
	private static final String LIST_OF_SENZA_DUPLICATI = "/home/chris88/Documenti/Tesi/wiki_tables/list_of/List_of_senza_duplicati.txt";
	
	public static void main(String[] args) {

		Map<Integer,Integer> positionDumpToNumberRows = new HashMap<Integer,Integer>();
		List<Integer> goldenSetIndex = new LinkedList<Integer>();

		BufferedReader readerStatisticalFile = null;

		try {
			readerStatisticalFile = new BufferedReader(new FileReader(new File(STATISTICAL_FILE)));
			String header = readerStatisticalFile.readLine();

			String line = "";
			while((line=readerStatisticalFile.readLine())!=null){
				String[] fieldsLine = line.split("\t");
				positionDumpToNumberRows.put(Integer.parseInt(fieldsLine[0]), Integer.parseInt(fieldsLine[6]));
			}

			int counter = 0;
			while(counter<1100){
				//l'intervallo del numero di righe è compreso tra 1 e 13525
				int k = (int)(Math.random()*13524)+1;
				if(positionDumpToNumberRows.containsKey(k)){
					if(positionDumpToNumberRows.get(k) >= 5){
						goldenSetIndex.add(k);
						positionDumpToNumberRows.remove(k);
						counter++;
					}
				}
			}

			Collections.sort(goldenSetIndex);
			
			for(Integer i: goldenSetIndex)
				System.out.println(i);
			
			
			BufferedReader readerDump = new BufferedReader(new FileReader(new File(LIST_OF_SENZA_DUPLICATI)));
			BufferedWriter writerGoldenSet = new BufferedWriter(new FileWriter(new File(GOLDEN_SET_DUMP)));
			int counterLine = 1;
			while((line= readerDump.readLine())!=null){
				if(goldenSetIndex.contains(counterLine)){
					writerGoldenSet.append(line+"\n");
					
				}
				counterLine++;
			}
			
			

			writerGoldenSet.flush();
			writerGoldenSet.close();
			readerDump.close();
			readerStatisticalFile.close();


		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}



	}

}
