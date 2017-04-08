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
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.gson.Gson;

public class CosineDistance {
	
	public static Reviewer baseReviewer;

	public static class CosineAllPairsMapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text values, Context context) throws IOException, InterruptedException 
		{
			String[] tokens = values.toString().split(",");
			//Writing user, book;rating
			context.write(new Text(tokens[0]), new Text(tokens[1] +";"+tokens[2]));
		}
	}

	public static class CosineTop10Users extends Reducer<Text, Text, Text, DoubleWritable> 
	{
		private HashMap<Reviewer, Double> cosinSimilarityMap = new HashMap<Reviewer, Double>();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			Reviewer comparedReviewer = new Reviewer(key.toString());			
			String[] br = new String[2]; //temporary array to split books and ratings
			HashMap<String,Double> comparedReviewer_bookAndRatings = new HashMap<String,Double>();
			HashSet<String> allBooks = new HashSet<String>();//Set of all the combine books of the base Reviewer and compared Reviewer

			allBooks.addAll(baseReviewer.ratings.keySet()); 			
			double comparedReviewerMean = 0;
			double rating;
			
			for(Text t : values)
			{
				br = t.toString().split(";");
				rating = Double.parseDouble(br[1]);
				comparedReviewer_bookAndRatings.put(br[0], rating);
				comparedReviewerMean += rating;
				allBooks.add(br[0]);
			}
			comparedReviewerMean = comparedReviewerMean/comparedReviewer_bookAndRatings.size();
			
			double normA =0;
			double normB =0;
			double dotProduct=0;
			double vectorA;
			double vectorB;
			for(String b : allBooks)
			{
				vectorA = baseReviewer.ratings.getOrDefault(b, (float)0) - baseReviewer.ratingMeans;
				vectorB = comparedReviewer_bookAndRatings.getOrDefault(b, (double)0) - comparedReviewerMean;
				
				dotProduct += vectorA * vectorB;
				normA += Math.pow(vectorA,2);
				normB += Math.pow(vectorB,2);
			}

			double cosineDistance = (dotProduct / (Math.sqrt(normA) * Math.sqrt(normB)));
		
			cosinSimilarityMap.put(comparedReviewer, cosineDistance);			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			//Used to sort the HashMap by descending order
			Comparator<Entry<Reviewer, Double>> valueComparator = new Comparator<Entry<Reviewer,Double>>() 
			{ 
				public int compare(Entry<Reviewer, Double> e1,Entry<Reviewer, Double> e2) 
				{
					double f1 = e1.getValue();
					double f2 = e2.getValue();
						
					if(f1 < f2) return 1;
					else if(f1 > f2) return -1;
					else return 0;
				}
			};
				
			Set<Entry<Reviewer, Double>> entries = cosinSimilarityMap.entrySet();
			ArrayList<Entry<Reviewer, Double>> listOfEntries =  new ArrayList<Entry<Reviewer, Double>>(entries);
			Collections.sort(listOfEntries, valueComparator);
	
			for(int i = 0; i < listOfEntries.size(); i++)
			{
				Entry<Reviewer, Double> entry = listOfEntries.get(i);
				String id = entry.getKey().id;
				Double cosin = entry.getValue();
				cosin = Math.round(cosin*10000d)/10000d;
					
				context.write(new Text(id), new DoubleWritable(cosin));
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		String fileInput = "file:///home//epar//workspace//AmazonBookReview//Data//Sample";
		String 	fileOutput = "file:///home//epar//workspace//AmazonBookReview//Data//Output";

		//String fileInput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Data//ALL-preprocessed";
		//String 	fileOutput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Output";
		
		Gson gson = new Gson();
		baseReviewer = generateBaseReviewer("user1");
		baseReviewer.calculateRatingMean();
		
		String ReviewerSerialization = gson.toJson(baseReviewer); //Serialize to pass to Reduce jobs	
		Configuration conf = new Configuration();
		conf.set("Reviewer", ReviewerSerialization);
		Job job = Job.getInstance(conf, "Cosine Similarity");
		job.setJarByClass(CosineDistance.class);
		job.setMapperClass(CosineAllPairsMapper.class);
		job.setReducerClass(CosineTop10Users.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(fileInput));
		FileOutputFormat.setOutputPath(job, new Path(fileOutput));
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
	
	public static Reviewer generateBaseReviewer(String id) throws IOException
	{		
		Reviewer Reviewer = new Reviewer(id);
		File data = new File("Data/Sample");
		BufferedReader br = new BufferedReader(new FileReader(data));
		String line;
		
		while((line = br.readLine()) != null)
		{
			String[] tokens = line.split(",");
			if(tokens[0].equals(Reviewer.id))
			{
				Reviewer.ratings.put(tokens[1], Float.parseFloat(tokens[2]));
				System.out.println(tokens[1]);
			}
		}
		return Reviewer;
	}
}

