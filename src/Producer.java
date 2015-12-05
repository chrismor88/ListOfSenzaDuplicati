
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class Producer extends Thread {

	private BlockingQueue<String> messageBuffer; //buffer in cui vengono trasmessi e prelevati le stringhe di tipo json
	private BlockingQueue<String> responseBuffer; //buffer per comunicare al produttore la terminazione dei consumatori
	private File workDirectory; //directory contenente i file json da processare
	private BufferedReader readerFile;
	private FiltroDuplicatiListOf filter;


	public Producer(File workDirectory,BlockingQueue<String> messageBuffer, BlockingQueue<String> responseBuffer) {
		this.messageBuffer = messageBuffer;
		this.responseBuffer = responseBuffer;
		this.workDirectory = workDirectory;
		this.filter = new FiltroDuplicatiListOf();

	}




	@Override
	public void run() {
		super.run();

		try {
			int currentLine = 1;
			readerFile = new BufferedReader(new FileReader(workDirectory));
			String line = "";
			while((line = readerFile.readLine())!=null) {
				messageBuffer.put(currentLine+"#"+line);
				currentLine ++;
			}
			readerFile = null;
			messageBuffer.put(Message.FINISHED_PRODUCER);

			int counterResponse = 0;
			while(true){
				String message = responseBuffer.take();
				if(message.equals(Message.FINISHED_CONSUMER))
					counterResponse ++;
				if(counterResponse == 1)
					break;
			}

			this.interrupt();

		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}


}



