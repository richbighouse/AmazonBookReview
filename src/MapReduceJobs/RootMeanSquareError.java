package MapReduceJobs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class RootMeanSquareError {
	
	public static HashMap<String, Double> actualRatings;
	public static HashMap<String, Double> estimatedRatings;

	public static void main(String[] args) throws IOException {
		String interestedUser = "user1";
		String fileRatings = "/home//rich//dev//workspaces//java8//AmazonBookReview//FinalOutput//part-r-00000";
		String fileInput = "/home//rich//dev//workspaces//java8//AmazonBookReview//Data//Sample";
		
		actualRatings = new HashMap<String, Double>();
		estimatedRatings = new HashMap<String, Double>();
		
		File actualData = new File(fileInput);
		BufferedReader br = new BufferedReader(new FileReader(actualData));
		String line;
		while((line = br.readLine()) != null){
			String[] tokens = line.split(","); // user,book,rating
			if(tokens[0].equals(interestedUser)){
				actualRatings.put(tokens[1], Double.parseDouble(tokens[2]));
			}
		}
		br.close();
		
		File estimatedData = new File(fileRatings);
		BufferedReader br2 = new BufferedReader(new FileReader(estimatedData));
		while((line = br2.readLine()) != null){
			String[] tokens = line.split("\t");
			estimatedRatings.put(tokens[0], Double.parseDouble(tokens[1]));
		}
		getRMSE(actualRatings, estimatedRatings);
	}
	
	public static void getRMSE(HashMap<String, Double> actualRatings, HashMap<String, Double> estimatedRatings) {
		double rmseSum = 0;
		for(String book : actualRatings.keySet()){
			double actualRating = actualRatings.get(book);
			double estimatedRating = estimatedRatings.get(book);
			rmseSum += Math.pow((estimatedRating - actualRating),2);		
		}
		System.out.println(Math.sqrt(rmseSum));
	}

}