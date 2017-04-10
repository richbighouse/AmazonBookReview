package MapReduceJobs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class Reviewer 
{
	public String id;
	public HashMap<String, Double> ratings;
	public HashMap<String, Double> hiddenRatings;
	public float ratingMeans = 0;
	public Task task;
	public static String DataPath = "";
	public static int topXUser = 10;
	public enum Task { JACCARD, COSINE, PEARSON}
	
	public Reviewer(String id, Task task)
	{
		this.id = id;
		this.task = task;
		this.ratings = new HashMap<String, Double>();
	}
	
	public Reviewer(String id)
	{
		this.id = id;
		this.ratings = new HashMap<String, Double>();
	}

	public static Reviewer generateBaseReviewer(String id, Reviewer.Task task) throws IOException
	{		
		Reviewer reviewer = new Reviewer(id,task);
		File data = new File(DataPath);
		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new FileReader(data));
		String line;
		
		while((line = br.readLine()) != null)
		{
			String[] tokens = line.split(",");
			if(tokens[0].equals(reviewer.id))
			{
				reviewer.ratings.put(tokens[1], Double.parseDouble(tokens[2]));
			}
		}
		reviewer.calculateRatingMean();
		
		return reviewer;
	}
	
	public void calculateRatingMean()
	{
		float temp = 0;
		for(String k : ratings.keySet())
		{
			temp += ratings.get(k);
		}
		ratingMeans = temp/ratings.size(); 
	}
	
	public static double calculateSimilarity(Reviewer baseReviewer, Reviewer comparedReviewer, HashSet<String> allBooks)
	{
		double similarity = 0;
		
		switch(baseReviewer.task)
		{
		case COSINE:
			similarity = cacluateCosineSimilarity(baseReviewer, comparedReviewer,allBooks);
			break;
		case JACCARD:
			similarity = calculateJaccardScore(baseReviewer, comparedReviewer, allBooks);
			break;
		case PEARSON:
			similarity = calculatePearsonSimilarity(baseReviewer, comparedReviewer, allBooks);
			break;
		}
		
		return similarity;
			
		
	}
	
	public static double cacluateCosineSimilarity(Reviewer baseReviewer, Reviewer comparedReviewer, HashSet<String> allBooks)
	{
		double cosineSimilarity = 0;	
		double normA =0;
		double normB =0;
		double dotProduct=0;
		double vectorA;
		double vectorB;
		
		for(String b : allBooks)
		{
			vectorA = baseReviewer.ratings.getOrDefault(b, (double)0);
			vectorB = comparedReviewer.ratings.getOrDefault(b, (double)0);
			
			dotProduct += vectorA * vectorB;
			normA += Math.pow(vectorA,2);
			normB += Math.pow(vectorB,2);
		}

		cosineSimilarity = (dotProduct / (Math.sqrt(normA) * Math.sqrt(normB)));
		
		return cosineSimilarity;		
	}
	
	public static double calculateJaccardScore(Reviewer baseReviewer, Reviewer comparedReviewer, HashSet<String> union)
	{
		double jaccardScore = 0;
		float numerator = 0;
		float denominator;

		for(String book : baseReviewer.ratings.keySet())
		{
			if(comparedReviewer.ratings.containsKey(book))
			{
				numerator++;
			}
		}
		denominator = union.size();
		jaccardScore = numerator/denominator;
		
		return jaccardScore;	
	}

	public static double calculatePearsonSimilarity(Reviewer baseReviewer, Reviewer comparedReviewer, HashSet<String> allBooks)
	{
		double pearsonSimilarity = -5;
			
		double normA =0;
		double normB =0;
		double dotProduct=0;
		double vectorA;
		double vectorB;
		
		for(String b : allBooks)
		{
			if(baseReviewer.ratings.containsKey(b) && comparedReviewer.ratings.containsKey(b))
			{
				vectorA = baseReviewer.ratings.get(b) - baseReviewer.ratingMeans;
				vectorB = comparedReviewer.ratings.get(b) - comparedReviewer.ratingMeans;
				
				dotProduct += vectorA * vectorB;
				normA += Math.pow(vectorA,2);
				normB += Math.pow(vectorB,2);
			}
		}
		
		if(dotProduct != 0)
			pearsonSimilarity = (dotProduct / (Math.sqrt(normA) * Math.sqrt(normB)));
		
		return pearsonSimilarity;	
	}
	
}
