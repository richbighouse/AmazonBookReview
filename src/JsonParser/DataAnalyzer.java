package JsonParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.gson.Gson;

public class DataAnalyzer {

	private final String DATAPATH = "Data/ALL-preprocessed";
	private final String NEWDATAPATH = "Data/modified-preprocessed";
	private static DataAnalyzer da = null;
	
	private HashMap<String, HashMap<String,Float>> allBookReviewsPerReviewer;
	private HashMap<String, HashMap<String,Float>> allReviewsPerBook;
	private int[] ratingDistribution;
	private int numberOfReviews;
	private int numberOfReviewers;
	private int numberOfBooks;
	private float sumOfAllRatings;
	
	private DataAnalyzer(){
		allBookReviewsPerReviewer = new HashMap<String, HashMap<String,Float>>();
		allReviewsPerBook = new HashMap<String, HashMap<String,Float>>();
		ratingDistribution = new int[6];
		numberOfReviews = 0;
		sumOfAllRatings = 0;
	}
	
	private void updateAttributes(String[] tokens){
		String reviewer = tokens[0];
		String book = tokens[1];
		float rating = Float.parseFloat(tokens[2]);
		
		HashMap<String, Float> bookRating;
		if(!allBookReviewsPerReviewer.containsKey(reviewer)){
			bookRating = new HashMap<String, Float>();
			allBookReviewsPerReviewer.put(reviewer, bookRating);
		} else{
			bookRating = allBookReviewsPerReviewer.get(reviewer);
		}
		bookRating.put(book, rating);
		
		HashMap<String, Float> reviewRating;
		if(!allReviewsPerBook.containsKey(book)){
			reviewRating = new HashMap<String, Float>();
			allReviewsPerBook.put(book, reviewRating);
		} else{
			reviewRating = allReviewsPerBook.get(book);
		}
		reviewRating.put(reviewer, rating);
		
		ratingDistribution[(int)rating]++;
		numberOfReviews++;
		sumOfAllRatings += rating;
		numberOfReviewers = allBookReviewsPerReviewer.size();
		numberOfBooks = allReviewsPerBook.size();
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
			System.out.println("["+i+"]: " + ratingDistribution[i] + "("+ratingDistribution[i]*100/numberOfReviews+"%)");
		}
	}
	
	private void displayRatingDistributionForReviewer(String reviewer){
		System.out.println("=== Reviewer: "+reviewer+" Rating Distribution ===");
		int[] distribution = new int[6];
		HashMap<String, Float> ratings = allBookReviewsPerReviewer.get(reviewer);	
		for(Entry<String,Float> entry : ratings.entrySet()){
			distribution[entry.getValue().intValue()]++;
		}
		for(int i = 1; i <= 5; i++){
			System.out.println("["+i+"]: " + distribution[i]);
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
			System.out.println("["+i+"]: " + distribution[i]);
		}
	}
	
	private LinkedHashMap<String, HashMap<String,Float>> orderReviewHashMap(HashMap<String, HashMap<String,Float>> hashMap){
		LinkedHashMap<String, HashMap<String,Float>> ordered = new LinkedHashMap<String, HashMap<String,Float>>();	
		Comparator<Entry<String, HashMap<String,Float>>> valueComparator = new Comparator<Entry<String, HashMap<String,Float>>>() { 
			public int compare(Entry<String, HashMap<String,Float>> e1,Entry<String, HashMap<String,Float>> e2) {
				int size1 = e1.getValue().size();
				int size2 = e2.getValue().size();
				return size2 - size1;
			}};
			ArrayList<Entry<String, HashMap<String,Float>>> listOfEntries = 
					new ArrayList<Entry<String, HashMap<String,Float>>>(hashMap.entrySet());
			Collections.sort(listOfEntries, valueComparator);
			
			for(Entry<String,HashMap<String,Float>> entry : listOfEntries){
				ordered.put(entry.getKey(), entry.getValue());
			}
			return ordered;
	}
	
	private void displayTopNFromOrdered(int n, LinkedHashMap<String, HashMap<String,Float>> list){
		System.out.println("=== Top " +n+ " most reviews ===");
		
		int i = 0;
		for(Entry<String, HashMap<String,Float>> entry:list.entrySet()){
			if(i >= n){
				return;
			}
			i++;
			System.out.println("["+ entry.getKey()+"] : " + entry.getValue().size());
		}	
	}
	
	public void generateTestSet(){
		
	}
		
	public static void main(String[] args) throws IOException {
		DataAnalyzer da = getInstance();	
		File data = new File(da.NEWDATAPATH);
		/*FileWriter fw = new FileWriter(da.NEWDATAPATH, true);*/
		BufferedReader br = new BufferedReader(new FileReader(data));
		String line;		
		while((line = br.readLine()) != null){		
			String[] tokens = line.split(",");
			da.updateAttributes(tokens);
		}
		
		System.out.println("Number of reviews: " + da.numberOfReviews);
		System.out.println("Number of reviewers: " + da.numberOfReviewers);
		System.out.println("Number of books: " + da.numberOfBooks);
		System.out.println("Average global rating: " + da.getAverageRatingTotal());
		System.out.println("Average reviews per reviewer: " + da.getAverageReviewsPerUser());
		System.out.println("Average reviews per book: " + da.getAverageReviewsPerBook());
		
		da.displayRatingDistribution();
		LinkedHashMap<String, HashMap<String,Float>> orderedReviewers = da.orderReviewHashMap(da.allBookReviewsPerReviewer);
		LinkedHashMap<String, HashMap<String,Float>> orderedBooks = da.orderReviewHashMap(da.allReviewsPerBook);
		da.displayTopNFromOrdered(10, orderedReviewers);
		da.displayTopNFromOrdered(10, orderedBooks);
		da.displayRatingDistributionForReviewer("A21NVBFIEQWDSG");
		
	}

}
