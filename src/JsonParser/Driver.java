package JsonParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import com.google.gson.*;

public class Driver {

	public static void main(String[] args) throws UnsupportedEncodingException, IOException {
		// TODO Auto-generated method stub
		
		Review review;
		Gson gson = new Gson();
		File data = new File("Data/Books_5.json");
		BufferedReader br = new BufferedReader(new FileReader(data));
		String line;
		HashMap<String, Integer> reviewsPerUser = new HashMap<String, Integer>();
		HashMap<String, Integer> reviewsPerItem = new HashMap<String, Integer>();
		HashMap<Float, Integer> ratingDistribution = new HashMap<Float, Integer>();
		
		float totalRating = 0;
		int numberOfReview = 0;
		int maxNumberOfReviewsForAUser = 0;
		int maxNumberOfReviewsForAnItem = 0;
		String maxNumberOfReviewsUser = null;
		String maxNumberOfReviewsItem = null;
		
		while((line = br.readLine()) != null){
			review = gson.fromJson(line, Review.class);
			review.getRating();
			numberOfReview++;
			totalRating += review.rating;
			if(totalRating < 1){ // just to print sample for analysis
				System.out.println(line + "\n");
			}
			
			if(!ratingDistribution.containsKey(review.rating)){
				ratingDistribution.put(review.rating, 1);
			} else{
				ratingDistribution.put(review.rating, ratingDistribution.get(review.rating) + 1);
			}
			
			if(!reviewsPerUser.containsKey(review.reviewerID)){
				reviewsPerUser.put(review.reviewerID, 1);
			} else{
				reviewsPerUser.put(review.reviewerID, reviewsPerUser.get(review.reviewerID) + 1);
			}
			if(reviewsPerUser.get(review.reviewerID) > maxNumberOfReviewsForAUser){
				maxNumberOfReviewsForAUser = reviewsPerUser.get(review.reviewerID);
				maxNumberOfReviewsUser = review.reviewerID;
			}	
			
			if(!reviewsPerItem.containsKey(review.asin)){
				reviewsPerItem.put(review.asin, 1);
			} else{
				reviewsPerItem.put(review.asin, reviewsPerItem.get(review.asin) + 1);
			}
			if(reviewsPerItem.get(review.asin) > maxNumberOfReviewsForAnItem){
				maxNumberOfReviewsForAnItem = reviewsPerItem.get(review.asin);
				maxNumberOfReviewsItem = review.asin;
			}
		}
		br.close();
		
		SortedSet<Float> keys = new TreeSet<Float>(ratingDistribution.keySet());
		for (Float key : keys) { 
		   Integer value = ratingDistribution.get(key);
		   double percentage = ((value * 100)/numberOfReview);
		   System.out.println(value + " ratings for score of " + key + ". (" + percentage + "%)");
		}
		
		System.out.println("\nNumber of reviews: " + numberOfReview);
		System.out.println("Number of reviewers: " + reviewsPerUser.size());
		System.out.println("Number of items: " + reviewsPerItem.size() + "\n");
		System.out.println("average rating: " + totalRating/numberOfReview);
		System.out.println("average ratings per user: " + (float)numberOfReview/reviewsPerUser.size());
		System.out.println("average ratings per item: " + (float)numberOfReview/reviewsPerItem.size());

		System.out.println(maxNumberOfReviewsUser + " is the user leading with " + maxNumberOfReviewsForAUser + " reviews.");
		System.out.println(maxNumberOfReviewsItem + " is the item leading with " + maxNumberOfReviewsForAnItem + " reviews.");
		}		
	}


