package MapReduceJobs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

public class RootMeanSquareError {
	
	public static HashMap<String, Double> actualRatings;
	public static HashMap<String, Double> estimatedRatings;

	public static void Execute(String fileRatings, Reviewer reviewer, String task, long startTime) throws NumberFormatException, IOException
	{
		estimatedRatings = new HashMap<String, Double>();
		
		File estimatedData = new File(fileRatings);
		BufferedReader br2 = new BufferedReader(new FileReader(estimatedData));
		String line;
		while((line = br2.readLine()) != null)
		{
			String[] tokens = line.split("\t");
			estimatedRatings.put(tokens[0], Double.parseDouble(tokens[1]));
		}
		//changer ici pour mettre le hidden ratings
		double rmse = getRMSE(reviewer.hiddenRatings, estimatedRatings);
	
		try(FileWriter fw = new FileWriter("Data//RMSE", true);
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter out = new PrintWriter(bw))
				{
			out.println("User: "+reviewer.id +"\tTask: " + task + "\tRMSE score: " + rmse +" Execution time: " + (System.currentTimeMillis() - startTime)/1000);
				} catch (IOException e) {
				}
	}
	
	public static void main(String[] args) throws IOException {
		String interestedUser = "user1";
		String fileRatings = "/home//rich//dev//workspaces//java8//AmazonBookReview//FinalOutput//part-r-00000";
		String fileInput = "/home//rich//dev//workspaces//java8//AmazonBookReview//Data//Sample";
		
		actualRatings = new HashMap<String, Double>();
		estimatedRatings = new HashMap<String, Double>();
		
		File actualData = new File(fileInput);
		BufferedReader br = new BufferedReader(new FileReader(actualData));
		String line;
		
		while((line = br.readLine()) != null)
		{
			String[] tokens = line.split(","); // user,book,rating
			if(tokens[0].equals(interestedUser))
			{
				actualRatings.put(tokens[1], Double.parseDouble(tokens[2]));
			}
		}
		br.close();
		
		File estimatedData = new File(fileRatings);
		BufferedReader br2 = new BufferedReader(new FileReader(estimatedData));
		while((line = br2.readLine()) != null)
		{
			String[] tokens = line.split("\t");
			estimatedRatings.put(tokens[0], Double.parseDouble(tokens[1]));
		}
		getRMSE(actualRatings, estimatedRatings);
	}
	
	public static double getRMSE(HashMap<String, Double> actualRatings, HashMap<String, Double> estimatedRatings) 
	{
		double rmseSum = 0;
		
		for(String book : actualRatings.keySet())
		{
			if(estimatedRatings.containsKey(book))
			{
				double actualRating = actualRatings.get(book);
				double estimatedRating = estimatedRatings.get(book);
				if(actualRating != Double.NaN && estimatedRating != Double.NaN)
					rmseSum += Math.pow((estimatedRating - actualRating),2);		
			}
			else
			{
				double actualRating = actualRatings.get(book);
				if(actualRating != Double.NaN)
					rmseSum += Math.pow((1 - actualRating),2);
			}				
		}
		rmseSum = rmseSum / actualRatings.size();
		rmseSum = Math.sqrt(rmseSum);
		return rmseSum;
	}

}
