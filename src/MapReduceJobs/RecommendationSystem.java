package MapReduceJobs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.gson.Gson;

public class RecommendationSystem {
	
	public static Reviewer baseReviewer;
	public static HashMap<Reviewer, Double> topReviewers;
	
	public static class RecommendationMapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text values, Context context) throws IOException, InterruptedException 
		{
			String[] tokens = values.toString().split(",");
			if(topReviewers.containsKey(tokens[0])){ //if userId is in topReviewers
				context.write(new Text(tokens[0]), new Text(tokens[1] + "," + tokens[2])); //userId, <bookId, bookRating>
			}
			else if(baseReviewer.id.equals(tokens[0])){ 
				baseReviewer.ratings.put(tokens[1], Double.valueOf(tokens[2]));
			}
		}
	}
	public static class RecommendationReducer extends Reducer<Text, Text, Text, FloatWritable> 
	{
		HashMap<String, HashMap<Double, Double>> bookRatings = new HashMap<String, HashMap<Double, Double>>();
		//HashMap<bookId, HashMap<bookRating, similartiryRating>>
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {
				Double similarity = topReviewers.get(key);
				
				for(Text value : values){
					String[] tokens = values.toString().split(","); //0 = bookId, 1 = bookRating
					if(!bookRatings.containsKey(tokens[0])){
						bookRatings.put(tokens[0], new HashMap<Double, Double>());
					}
					bookRatings.get(tokens[0]).put(Double.valueOf(tokens[1]), similarity);
				}			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String fileInput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Data//ALL-preprocessed";
		String topNUsersInput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Output//part-r-00000";
		String fileOutput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//FinalOutput";
		File outputFolder = new File("//home//rich//dev//workspaces//java8//AmazonBookReview//FinalOutput");
				
		if(outputFolder.exists())
		{
			FileUtils.deleteDirectory(outputFolder);
		}		
		File data = new File(topNUsersInput);
		BufferedReader br = new BufferedReader(new FileReader(data));
		String line;		
		while((line = br.readLine()) != null){		
			String[] tokens = line.split(",");
			topReviewers.put(new Reviewer(tokens[0]), Double.valueOf(tokens[1]));
		}
		
		Gson gson = new Gson();
		baseReviewer = new Reviewer("A1ZG43T5D0TILP");
		//String reviewerSerialization = gson.toJson(baseReviewer); //Serialize to pass to Reduce jobs	
		Configuration conf = new Configuration();
		//conf.set("reviewer", reviewerSerialization);
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

	public static Reviewer generateBaseReviewer(String id) throws IOException{		
		Reviewer reviewer = new Reviewer(id);
		File data = new File("Data/processedReview");
		BufferedReader br = new BufferedReader(new FileReader(data));
		String line;
		
		while((line = br.readLine()) != null){
			String[] tokens = line.split(",");
			if(tokens[0].equals(reviewer.id)){
				reviewer.ratings.put(tokens[1], Double.parseDouble(tokens[2]));
				System.out.println(tokens[1]);
			}
		}
		return reviewer;
	}
}

