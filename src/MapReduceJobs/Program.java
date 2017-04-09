package MapReduceJobs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;

import JsonParser.TestSet;
import JsonParser.TestUser;

public class Program {
/*This class serves as the entry point for all our tasks
 * 
 * 
 * */
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		// TODO Auto-generated method stub
		//Fetch Users
		
		String fileInput = "file:///home//epar//workspace//AmazonBookReview//Data//modified-preprocessed";
		String fileOutput = "file:///home//epar//workspace//AmazonBookReview//Data//Output";
		//String fileInput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Data//modified-preprocessed";
		//String fileOutput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Output";

		
		ArrayList<Threshold> thresholds = new ArrayList<Threshold>(3)
		{
			{
				add(new Threshold(true, 2)); 
				add(new Threshold(true, 5)); 
				add(new Threshold(false, 3));
			}
		};
		
		ArrayList<Reviewer.Task> tasks= new ArrayList<Reviewer.Task>(3)
		{
			{
				add(Reviewer.Task.JACCARD); 
				add(Reviewer.Task.COSINE); 
				add(Reviewer.Task.PEARSON);
			}
		};		
		ArrayList<Reviewer> reviewers = null;
		
		
		
		//for user : users
		ArrayList<TestUser> users = TestSet.deserializeTestUsers("Data/testusers.ser");
		
		Reviewer reviewer = null;
		String fileOutputtemp;
		for(TestUser user: users)
		{	
			fileOutputtemp = fileOutput;
			System.out.println("Beginning tasks for user: " + user.id);
			fileOutputtemp += ("_"+user.id + "//");
			for(Reviewer.Task task: tasks)
			{
				reviewer = new Reviewer(user.id, task);
				reviewer.topXUser = 20;
				fileOutputtemp += task.toString();
				
				for(Threshold thres : thresholds)
				{
					reviewer.ratings = TestSet.getSetWithThreshold(thres,user.baseSet);
					reviewer.hiddenRatings = TestSet.getSetWithThreshold(thres,user.hiddenSet);
					reviewer.calculateRatingMean();
	
					System.out.println("Beginning task "+ task.toString() + " threshold " + thres.threshold);
					fileOutputtemp += ("_"+thres.threshold);

					System.out.println("Distance Measurement start.");

					DistanceMeasurementMapper.Execute(fileInput, fileOutputtemp, thres, reviewer);
					
					System.out.println("Distance Measurement done.\n Recommendation system start.");
					
					RecommendationSystem.Execute(fileInput, fileOutputtemp+"_RS", fileOutputtemp.replace("file:", "")+"//part-r-00000", reviewer);
					
					System.out.println("Recommendation System done. \n Root mean square start.");
					
					RootMeanSquareError.Execute(fileOutputtemp.replace("file:", "")+"_RS//part-r-00000", reviewer);
					
					System.out.println("RMSE Done.\n\n\n");
					
					fileOutputtemp = fileOutputtemp.replace(("_"+thres.threshold), "");
				}
				fileOutputtemp = fileOutput.replace(task.toString(), "");
			}
			break;
		}
	}

}
