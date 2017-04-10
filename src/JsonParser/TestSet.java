package JsonParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import MapReduceJobs.Threshold;

import com.google.gson.Gson;

public class TestSet {

	public static ArrayList<TestUser> testUsers;
	public static HashMap<String, HashMap<String, Double>> allReviewers;
	
	public static void main(String[] args) throws IOException {
		allReviewers = new HashMap<String, HashMap<String, Double>>();		
		testUsers = new ArrayList<TestUser>();
		
		String dataPath = "Data/preprocessed";
		File data = new File(dataPath);
		BufferedReader br = new BufferedReader(new FileReader(data));
		String line;		
		while((line = br.readLine()) != null){		
			String[] tokens = line.split(",");
			String user = tokens[0];
			if(!allReviewers.containsKey(user)){
				allReviewers.put(user, new HashMap<String,Double>());
			}
			allReviewers.get(user).put(tokens[1], Double.parseDouble(tokens[2]));
		}
		
		generateTestUsers(50);
		serializeTestUsers();
		
		try {
			ArrayList<TestUser> deserialized = deserializeTestUsers("Data/testusers.ser");
			int count = 1;
			for(TestUser user : deserialized){
				System.out.println(count++ + " " + user.id + ", number of books: " + (user.baseSet.size() + user.hiddenSet.size()));
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void generateTestUsers(int testSize) {
		
		Random random = new Random();
		
		for(int i = 0; i < testSize; i++){
			ArrayList<String> keys = new ArrayList<String>(allReviewers.keySet());
			String randomKey = keys.get(random.nextInt(keys.size()));
			HashMap<String, Double> value = allReviewers.get(randomKey);
			if(value.size() > 29 && value.size() < 150 && (value.containsValue(2.0) || value.containsValue(1.0))){
				TestUser testUser = new TestUser(randomKey);
				testUser.generateSets(value);
				testUsers.add(testUser);
			} else {
				i--;
			}
		}
	}
	
	public static void serializeTestUsers(){
		try {
            FileOutputStream fileOut = new FileOutputStream("Data/testusersoriginal.ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(testUsers);
            out.close();
            fileOut.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }		
	}
	
	public static ArrayList<TestUser> deserializeTestUsers(String path) throws IOException, ClassNotFoundException{
        FileInputStream fileIn = new FileInputStream(path);
        ObjectInputStream in = new ObjectInputStream(fileIn);
        ArrayList<TestUser> testUserList = (ArrayList<TestUser>) in.readObject(); 
        in.close();
        fileIn.close();
        return testUserList;
	}
	
	public static HashMap<String, Double> getSetWithThreshold(Threshold t,HashMap<String, Double> set)
	{
		double rating;
		HashMap<String, Double> temp = new HashMap<String, Double>();
		
		for(String s : set.keySet())
		{
			rating = set.get(s);
			if(t.smallerThan)
			{
				if(rating <= t.threshold)
				{
					temp.put(s, rating);
				}
			}
			else
			{
				if(rating >= t.threshold)
				{
					temp.put(s, rating);
				}
			}
		}
		
		return temp;
	}
}

