package MapReduceJobs;

import java.awt.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.gson.Gson;

public class JaccardSimilarityJobs {
	
	public static Reviewer baseReviewer;

	public static class JaccardAllPairsMapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text values, Context context) throws IOException, InterruptedException 
		{
			String[] tokens = values.toString().split(",");
			/* if applicable, enter rating conditions here */
			context.write(new Text(tokens[0]), new Text(tokens[1]));
		}
	}

	public static class JaccardTop10Users extends Reducer<Text, Text, Text, FloatWritable> 
	{
		private HashMap<Reviewer, Float> jaccardSimilarityMap = new HashMap<Reviewer, Float>();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			float numerator = 0;
			float denominator;
			float jaccardSimilarity;
			HashSet<String> union = new HashSet<String>();
			Reviewer comparedReviewer = new Reviewer(key.toString());
			
			union.addAll(baseReviewer.ratings.keySet());
			for(Text t : values){
				String book = t.toString();
				union.add(book);
				comparedReviewer.ratings.put(book, (float) 1);
				if(baseReviewer.ratings.keySet().contains(book)){
					numerator++;
				}
			}
			denominator = union.size();
			jaccardSimilarity = numerator/denominator;
			jaccardSimilarityMap.put(comparedReviewer, jaccardSimilarity);			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			
			//Used to sort the HashMap by descending order
			Comparator<Entry<Reviewer, Float>> valueComparator = new Comparator<Entry<Reviewer,Float>>() { 
				
				public int compare(Entry<Reviewer, Float> e1,Entry<Reviewer, Float> e2) {
					float f1 = e1.getValue();
					float f2 = e2.getValue();
					float answer = f1-f2;
					if(answer < 0){
						return 1;
					} else if(answer > 0){
						return -1;
					} else return 0;
				}};
				Set<Entry<Reviewer, Float>> entries = jaccardSimilarityMap.entrySet();
				ArrayList<Entry<Reviewer, Float>> listOfEntries =  new ArrayList<Entry<Reviewer, Float>>(entries);
				Collections.sort(listOfEntries, valueComparator);
	
				int numberOfReviewersToConsider = 10; //change this value to the desired number of Reviewers to consider
				for(int i = 0; i < listOfEntries.size(); i++){
					Entry<Reviewer, Float> entry = listOfEntries.get(i);
					String id = entry.getKey().id;
					Float jaccard = entry.getValue();
					if(jaccard > 0)
						context.write(new Text(id), new FloatWritable(jaccard));
				}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		//String fileInput = "file:///home//epar//workspace//AmazonBookReview//Data//processedReview";
		//String 	fileOutput = "file:///home//epar//workspace//AmazonBookReview//Data//Output";

		String fileInput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Data//ALL-preprocessed";
		String 	fileOutput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Output";
		
		Gson gson = new Gson();
		baseReviewer = generateBaseReviewer("A1VOONMRQZUBE4");
		String reviewerSerialization = gson.toJson(baseReviewer); //Serialize to pass to Reduce jobs	
		Configuration conf = new Configuration();
		conf.set("reviewer", reviewerSerialization);
		Job job = Job.getInstance(conf, "Jaccard Similarity");
		job.setJarByClass(JaccardSimilarityJobs.class);
		job.setMapperClass(JaccardAllPairsMapper.class);
		job.setReducerClass(JaccardTop10Users.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(fileInput));
		FileOutputFormat.setOutputPath(job, new Path(fileOutput));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
	public static Reviewer generateBaseReviewer(String id) throws IOException{		
		Reviewer reviewer = new Reviewer(id);
		File data = new File("Data/processedReview");
		BufferedReader br = new BufferedReader(new FileReader(data));
		String line;
		
		while((line = br.readLine()) != null){
			String[] tokens = line.split(",");
			if(tokens[0].equals(reviewer.id)){
				reviewer.ratings.put(tokens[1], Float.parseFloat(tokens[2]));
				System.out.println(tokens[1]);
			}
		}
		return reviewer;
	}
}