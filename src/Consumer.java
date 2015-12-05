

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;





public class Consumer extends Thread {

	private BlockingQueue<String> messageBuffer; //buffer in cui vengono trasmessi e prelevati le stringhe di tipo json
	private BlockingQueue<String> outputBuffer; //buffer per comunicare al produttore la terminazione dei consumatori
	private Map<String, Pair<Integer,Integer>>  title_nrows_nline;
	private Main main;




	public Consumer(BlockingQueue<String> messageBuffer, BlockingQueue<String> responseBuffer,
			Map<String, Pair<Integer,Integer>> title_nrows_nline, Main main) {
		this.messageBuffer = messageBuffer;
		this.outputBuffer = responseBuffer;
		this.title_nrows_nline = title_nrows_nline;
		this.main = main;
	}


	@Override
	public void run() {
		super.run();

		while(true){

			try {
				String message = messageBuffer.take();
				if(message.equals(Message.FINISHED_PRODUCER)){
					break;
				}
				else{
					String[] messageSplitted = message.split("#");
					int numberOfLine = Integer.parseInt(messageSplitted[0]);
					JSONParser jsonParser = new JSONParser();
					JSONObject jsonObject = (JSONObject) jsonParser.parse(messageSplitted[1]);

					String pgTitle = (String)jsonObject.get("pgTitle");
					JSONArray tableData = (JSONArray)jsonObject.get("tableData");
					String wikid = pgTitle.replaceAll(" ","_");
					int numberOfRows = tableData.size();

					synchronized (title_nrows_nline) {
						if(title_nrows_nline.containsKey(wikid)){
							int n = title_nrows_nline.get(wikid).getFirst();
							if(n < numberOfRows){
								title_nrows_nline.put(wikid, new Pair<Integer,Integer>(numberOfRows,numberOfLine));
							}
						}
						else{
							title_nrows_nline.put(wikid, new Pair<Integer,Integer>(numberOfRows,numberOfLine));
						}
					}

					main.incrementCounter();
					System.out.println(main.getCounter());
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			} 
		}


		try {
			messageBuffer.put(Message.FINISHED_PRODUCER);
			outputBuffer.put(Message.FINISHED_CONSUMER);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}


		this.interrupt();

	}



}
