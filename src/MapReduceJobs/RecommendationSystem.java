package MapReduceJobs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RecommendationSystem {

	public static Reviewer baseReviewer;
	public static HashMap<String, Double> topReviewers;

	public static class RecommendationMapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text values, Context context) throws IOException, InterruptedException 
		{
			String[] tokens = values.toString().split(",");
			if(topReviewers.containsKey(tokens[0]))//if userId is in topReviewers
			{ 
				context.write(new Text(tokens[0]), new Text(tokens[1] + ";" + tokens[2])); //userId, <bookId, bookRating>
			}
			else if(baseReviewer.id.equals(tokens[0]))
			{ 
				baseReviewer.ratings.put(tokens[1], Double.valueOf(tokens[2]));
			}
		}
	}
	
	public static class RecommendationReducer extends Reducer<Text, Text, Text, DoubleWritable> 
	{
		HashMap<String, ArrayList<Tuple<Double, Double>>> bookRatings = new HashMap<String, ArrayList<Tuple<Double, Double>>>();
		//HashMap<bookId, HashMap<bookRating, similartiryRating>>
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException 
		{
			Double similarity = topReviewers.get(key.toString());			
			for(Text value : values){
				String[] tokens = value.toString().split(";"); //0 = bookId, 1 = bookRating
				if(!bookRatings.containsKey(tokens[0])){
					bookRatings.put(tokens[0], new ArrayList<Tuple<Double,Double>>());
				}
				ArrayList<Tuple<Double, Double>> listOfRatings = bookRatings.get(tokens[0]);
				listOfRatings.add(new Tuple<Double,Double>(Double.parseDouble(tokens[1]), similarity));
			}			
		}

		public void cleanup(Context context) throws IOException, InterruptedException
		{
			double sum;
			double totalSimilarity;
			for(String book : bookRatings.keySet())
			{
				sum = 0;
				totalSimilarity = 0;
				for(Tuple<Double, Double> ratings : bookRatings.get(book))
				{
					sum += ratings.x * ratings.y;
					totalSimilarity += ratings.y;
				}				
				context.write(new Text(book), new DoubleWritable(sum/totalSimilarity));
			}		
		}
	}

	public static void Execute (String fileInput, String fileOutput, String topNUsersInput, Reviewer reviewer) throws IOException, ClassNotFoundException, InterruptedException
	{
		baseReviewer = reviewer;
		
		File outputFolder = new File(fileOutput.replace("file://", ""));
		
		if(outputFolder.exists())
		{
			FileUtils.deleteDirectory(outputFolder);
		}	
		
		File data = new File(topNUsersInput);
		BufferedReader br = new BufferedReader(new FileReader(data));
		String line;
		double temp;
		topReviewers = new HashMap<String, Double>();
		
		while((line = br.readLine()) != null)
		{		
			String[] tokens = line.split("\\t");
			temp = Double.valueOf(tokens[1]);
			if(!tokens[0].equals(baseReviewer.id))
			{
				topReviewers.put(tokens[0], temp);
			}
		}

		Configuration conf = new Configuration();
		Job secondJob = Job.getInstance(conf, "Recommendations");
		secondJob.setJarByClass(RecommendationSystem.class);
		secondJob.setMapperClass(RecommendationMapper.class);
		secondJob.setReducerClass(RecommendationReducer.class);
		secondJob.setOutputKeyClass(Text.class);
		secondJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(secondJob, new Path(fileInput));
		FileOutputFormat.setOutputPath(secondJob, new Path(fileOutput));
		secondJob.waitForCompletion(true);

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		String interestedUser = "user1";
		String fileInput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Data//Sample";
		String topNUsersInput = "/home//rich//dev//workspaces//java8//AmazonBookReview//Output//part-r-00000";
		String fileOutput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//FinalOutput";
		/*
		String fileInput = "file:///home//epar//workspace//AmazonBookReview//Data//Sample";
		String fileOutput = "file:///home//epar//workspace//AmazonBookReview//Data//FinalOutput";;
		String topNUsersInput = "/home//epar//workspace//AmazonBookReview//Data//Output//part-r-00000";
		*/
		
		baseReviewer = new Reviewer(interestedUser);

		File outputFolder = new File(fileOutput.replace("file://", ""));
		
		if(outputFolder.exists())
		{
			FileUtils.deleteDirectory(outputFolder);
		}	

		File data = new File(topNUsersInput);
		BufferedReader br = new BufferedReader(new FileReader(data));
		String line;
		double temp;
		topReviewers = new HashMap<String, Double>();
		
		while((line = br.readLine()) != null)
		{		
			String[] tokens = line.split("\\t");
			temp = Double.valueOf(tokens[1]);
			if(!tokens[0].equals(baseReviewer.id))
			{
				topReviewers.put(tokens[0], temp);
			}
		}

		Configuration conf = new Configuration();
		Job secondJob = Job.getInstance(conf, "Recommendations");
		secondJob.setJarByClass(RecommendationSystem.class);
		secondJob.setMapperClass(RecommendationMapper.class);
		secondJob.setReducerClass(RecommendationReducer.class);
		secondJob.setOutputKeyClass(Text.class);
		secondJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(secondJob, new Path(fileInput));
		FileOutputFormat.setOutputPath(secondJob, new Path(fileOutput));
		System.exit(secondJob.waitForCompletion(true) ? 0 : 1);		
	}

}


