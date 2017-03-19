package JsonParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;

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
		
		float totalRating = 0;
		int numberOfReview = 0;
		int maxNumberOfReviewsForAUser = 0;
		String maxNumberOfReviewsUser = null;
		
		while((line = br.readLine()) != null){
			review = gson.fromJson(line, Review.class);
			review.getRating();
			numberOfReview++;
			totalRating += review.rating;	
			
			if(!reviewsPerUser.containsKey(review.reviewerID)){
				reviewsPerUser.put(review.reviewerID, 1);
			} else{
				reviewsPerUser.put(review.reviewerID, reviewsPerUser.get(review.reviewerID) + 1);
			}
			if(reviewsPerUser.get(review.reviewerID) > maxNumberOfReviewsForAUser){
				maxNumberOfReviewsForAUser = reviewsPerUser.get(review.reviewerID);
				maxNumberOfReviewsUser = review.reviewerID;
			}			
		}
		br.close();
		
		System.out.println("Number of reviews: " + numberOfReview);
		System.out.println("average rating: " + totalRating/numberOfReview);
		System.out.println(maxNumberOfReviewsUser + " is leading with " + maxNumberOfReviewsForAUser + " reviews.");
		System.out.println(maxNumberOfReviewsUser + " is leading with " + maxNumberOfReviewsForAUser + " reviews.");
		}		
	}

