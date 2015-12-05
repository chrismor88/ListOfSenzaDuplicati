
import java.util.LinkedHashSet;
import java.util.Set;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class FiltroDuplicatiListOf {

	private Set<String> tabelleProcessate;

	public FiltroDuplicatiListOf() {
		tabelleProcessate = new LinkedHashSet<>();
	}

	public boolean valutaTabella(String table){
		boolean response = false;
		JSONParser parser = new JSONParser();
		try {
			JSONObject jsonObject = (JSONObject) parser.parse(table);
			String pgTitle = (String)jsonObject.get("pgTitle");
			if(!tabelleProcessate.contains(pgTitle)){
				tabelleProcessate.add(pgTitle);
				response = true;
			}

		} catch (ParseException e) {
			e.printStackTrace();
		}

		return response;

	}

}
