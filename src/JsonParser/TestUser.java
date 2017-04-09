package JsonParser;

import java.io.Serializable;
import java.util.HashMap;

public class TestUser implements Serializable {
	
	public String id;
	public HashMap<String, Double> baseSet;
	public HashMap<String, Double> hiddenSet;
	
	public TestUser(String id){
		this.id = id;
		baseSet = new HashMap<String, Double>();
		hiddenSet = new HashMap<String, Double>();
	}
	
	public void generateSets(HashMap<String, Double> fullSet){
		int size = fullSet.size();
		int baseSize = (int) (size * 0.8);
	
		for(String book : fullSet.keySet()){
			if(baseSize > 0){
				baseSet.put(book, fullSet.get(book));
				baseSize--;
			} else {
				hiddenSet.put(book, fullSet.get(book));
			}
		}
	}
}
