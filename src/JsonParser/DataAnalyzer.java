package JsonParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.gson.Gson;

public class DataAnalyzer {

	private final String DATAPATH = "Data/ALL-preprocessed";
	private static DataAnalyzer da = null;
	
	private HashMap<String, HashMap<String,Float>> allBookReviewsPerReviewer;
	private HashMap<String, HashMap<String,Float>> allReviewsPerBook;
	private int[] ratingDistribution;
	private int numberOfReviews;
	private int numberOfReviewers;
	private int numberOfBooks;
	private int sumOfAllRatings;
	
	private DataAnalyzer(){
		allBookReviewsPerReviewer = new HashMap<String, HashMap<String,Float>>();
		allReviewsPerBook = new HashMap<String, HashMap<String,Float>>();
		ratingDistribution = new int[6];
		numberOfReviews = 0;
		sumOfAllRatings = 0;
	}
	
	private static DataAnalyzer getInstance(){
		if(da == null){
			da = new DataAnalyzer();
		}
		return da;
	}	
	
	private float getAverageRatingTotal(){
		return (float)sumOfAllRatings/numberOfReviews;
	}	
	
	private float getAverageReviewsPerUser(){
		return (float)numberOfReviews/numberOfReviewers;
	}
	
	private float getAverageReviewsPerBook(){
		return (float)numberOfReviews/numberOfBooks;
	}
	
	private void displayRatingDistribution(){
		System.out.println("=== Global Rating Distribution ===");
		for(int i = 1; i <= 5; i++){
			System.out.println("[1] :" + ratingDistribution[i]);
		}
	}
	
	private void displayRatingDistributionForReviewer(String reviewer){
		System.out.println("=== Reviwer: "+reviewer+" Rating Distribution ===");
		int[] distribution = new int[6];
		HashMap<String, Float> ratings = allBookReviewsPerReviewer.get(reviewer);	
		for(Entry<String,Float> entry : ratings.entrySet()){
			distribution[entry.getValue().intValue()]++;
		}
		for(int i = 1; i <= 5; i++){
			System.out.println("[1] :" + distribution[i]);
		}
	}
	
	private void displayRatingDistributionForBook(String book){
		System.out.println("=== Book: "+book+" Rating Distribution ===");
		int[] distribution = new int[6];
		HashMap<String, Float> ratings = allReviewsPerBook.get(book);	
		for(Entry<String,Float> entry : ratings.entrySet()){
			distribution[entry.getValue().intValue()]++;
		}
		for(int i = 1; i <= 5; i++){
			System.out.println("[1] :" + distribution[i]);
		}
	}
	
	

	
	public static void main(String[] args) throws IOException {
		DataAnalyzer da = getInstance();	
		File data = new File(da.DATAPATH);
		BufferedReader br = new BufferedReader(new FileReader(data));
		String line;
		
		while((line = br.readLine()) != null){
			
		}

	}

}
