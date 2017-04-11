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
		
		String fileInput = "file:///home//epar//workspace//AmazonBookReview//Data//preprocessed";
		String fileOutput = "file:///home//epar//workspace//AmazonBookReview//Data//Output";
		//String fileInput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Data//modified-preprocessed";
		//String fileInput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Data//preprocessed";
		//String fileOutput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Output";

		ArrayList<Threshold> thresholds = new ArrayList<Threshold>(3)
		{
			{
				add(new Threshold(true, 3)); 
				add(new Threshold(true, 5)); 
				add(new Threshold(false, 4));
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
		ArrayList<TestUser> users = TestSet.deserializeTestUsers("Data/testusersoriginal.ser");

		/*ArrayList<TestUser> users = new ArrayList<TestUser>();
		TestUser user1 = new TestUser("user1");
		user1.baseSet.put("book10",1.0);
		user1.baseSet.put("book9",1.0);
		user1.baseSet.put("book8",3.0);
		user1.baseSet.put("book7",5.0);
		user1.baseSet.put("book6",5.0);
		user1.baseSet.put("book5",5.0);
		user1.baseSet.put("book4",5.0);
		user1.hiddenSet.put("book3",5.0);
		user1.hiddenSet.put("book2",5.0);
		user1.hiddenSet.put("book1",5.0);
		users.add(user1);*/		
		
		Reviewer reviewer = null;
		String fileOutputtemp;
		
		
		if(new File("Data//RMSE").exists())
		{
			new File("Data//RMSE").delete();
		}
		
		int i = 0;
		for(TestUser user: users)
		{	/*
			if(!user.id.equals("ALCC6SWW4WCXR")){
				continue;
			}*/
			fileOutputtemp = fileOutput;
			System.out.println("Beginning tasks for user: " + user.id);
			fileOutputtemp += ("_"+user.id + "//");
			long startTime;
			for(Reviewer.Task task: tasks)
			{
				/*if(!task.equals(Reviewer.Task.PEARSON)){
					continue;
				}*/
				reviewer = new Reviewer(user.id, task);
				reviewer.topXUser = 50;
				fileOutputtemp += task.toString();
				
				for(Threshold thres : thresholds)
				{
					reviewer.ratings = user.baseSet;
					reviewer.hiddenRatings = user.hiddenSet;
					

					if(reviewer.ratings.size() > 0 && reviewer.hiddenRatings.size()>0)
					{
						startTime = System.currentTimeMillis();
						System.out.println("Beginning task "+ task.toString() + " threshold " + thres.threshold);
						fileOutputtemp += ("_"+thres.threshold);
						System.out.println("Distance Measurement start.");
						DistanceMeasurementMapper.Execute(fileInput, fileOutputtemp, thres, reviewer);
						System.out.println("Distance Measurement done.\n Recommendation system start.");
						RecommendationSystem.Execute(fileInput, fileOutputtemp+"_RS", fileOutputtemp.replace("file:", "")+"//part-r-00000", reviewer);						
						System.out.println("Recommendation System done. \n Root mean square start.");		
						RootMeanSquareError.Execute(fileOutputtemp.replace("file:", "")+"_RS//part-r-00000", reviewer,(task+"_"+((thres.smallerThan)?"<=":">=")+thres.threshold),startTime);						
						System.out.println("RMSE Done.\n\n\n");
					
						fileOutputtemp = fileOutputtemp.replace(("_"+thres.threshold), "");
					}
				}
				fileOutputtemp = fileOutputtemp.replace(task.toString(), "");
			}
			i++;
			if(i== 1)
			{
				System.exit(0);
			}
		}
	}

}
