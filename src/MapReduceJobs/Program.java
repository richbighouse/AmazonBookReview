package MapReduceJobs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;

public class Program {
/*This class serves as the entry point for all our tasks
 * 
 * 
 * */
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException 
	{
		// TODO Auto-generated method stub
		//Fetch Users
		
		String fileInput = "file:///home//epar//workspace//AmazonBookReview//Data//Sample";
		String fileOutput = "file:///home//epar//workspace//AmazonBookReview//Data//Output";
		//String fileInput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Data//Sample";
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
		for(Reviewer reviewer: reviewers)
		{
			for(Reviewer.Task task: tasks)
			{
				fileOutput += task.toString();
				for(Threshold thres : thresholds)
				{
					fileOutput += ("_"+thres.threshold);
					//Reviewer.topXUser = 4;
					//		Reviewer.Task task = Reviewer.Task.COSINE;
						//generateBaseReviewer ??
					DistanceMeasurementMapper.Execute(fileInput, fileOutput, thres, reviewer);
			
					RecommendationSystem.Execute(fileInput, fileOutput+"_RS", fileOutput+"//part-r-00000", reviewer);
					
					RootMeanSquareError.Execute(fileOutput+"_RS//part-r-00000", reviewer);
					fileOutput.replace(("_"+thres.threshold), "");
				}
				fileOutput.replace(task.toString(), "");
			}
		}
	}

}
