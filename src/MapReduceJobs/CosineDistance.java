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
	
	public static Reviewer2 baseReviewer2;

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
		private HashMap<Reviewer2, Double> cosinSimilarityMap = new HashMap<Reviewer2, Double>();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			Reviewer2 comparedReviewer2 = new Reviewer2(key.toString());			
			String[] br = new String[2]; //temporary array to split books and ratings
			HashMap<String,Double> comparedReviewer2_bookAndRatings = new HashMap<String,Double>();
			HashSet<String> allBooks = new HashSet<String>();//Set of all the combine books of the base Reviewer2 and compared Reviewer2

			allBooks.addAll(baseReviewer2.ratings.keySet()); 			
			double comparedReviewer2Mean = 0;
			double rating;
			
			for(Text t : values)
			{
				br = t.toString().split(";");
				rating = Double.parseDouble(br[1]);
				comparedReviewer2_bookAndRatings.put(br[0], rating);
				comparedReviewer2Mean += rating;
				allBooks.add(br[0]);
			}
			comparedReviewer2Mean = comparedReviewer2Mean/comparedReviewer2_bookAndRatings.size();
			
			double normA =0;
			double normB =0;
			double dotProduct=0;
			double vectorA;
			double vectorB;
			for(String b : allBooks)
			{
				vectorA = baseReviewer2.ratings.getOrDefault(b, (float)0) - baseReviewer2.ratingMeans;
				vectorB = comparedReviewer2_bookAndRatings.getOrDefault(b, (double)0) - comparedReviewer2Mean;
				
				dotProduct += vectorA * vectorB;
				normA += Math.pow(vectorA,2);
				normB += Math.pow(vectorB,2);
			}

			double cosineDistance = (dotProduct / (Math.sqrt(normA) * Math.sqrt(normB)));
		
			cosinSimilarityMap.put(comparedReviewer2, cosineDistance);			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			//Used to sort the HashMap by descending order
			Comparator<Entry<Reviewer2, Double>> valueComparator = new Comparator<Entry<Reviewer2,Double>>() 
			{ 
				public int compare(Entry<Reviewer2, Double> e1,Entry<Reviewer2, Double> e2) 
				{
					double f1 = e1.getValue();
					double f2 = e2.getValue();
						
					if(f1 < f2) return 1;
					else if(f1 > f2) return -1;
					else return 0;
				}
			};
				
			Set<Entry<Reviewer2, Double>> entries = cosinSimilarityMap.entrySet();
			ArrayList<Entry<Reviewer2, Double>> listOfEntries =  new ArrayList<Entry<Reviewer2, Double>>(entries);
			Collections.sort(listOfEntries, valueComparator);
	
			for(int i = 0; i < listOfEntries.size(); i++)
			{
				Entry<Reviewer2, Double> entry = listOfEntries.get(i);
				String id = entry.getKey().id;
				Double cosin = entry.getValue();
				cosin = Math.round(cosin*10000d)/10000d;
					
				context.write(new Text(id), new DoubleWritable(cosin));
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		String fileInput = "file:///home//epar//workspace//AmazonBookReview//Data//processedReview";
		String 	fileOutput = "file:///home//epar//workspace//AmazonBookReview//Data//Output";

		//String fileInput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Data//ALL-preprocessed";
		//String 	fileOutput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Output";
		
		Gson gson = new Gson();
		baseReviewer2 = generateBaseReviewer2("A2S166WSCFIFP5");
		baseReviewer2.calculateRatingMean();
		
		String Reviewer2Serialization = gson.toJson(baseReviewer2); //Serialize to pass to Reduce jobs	
		Configuration conf = new Configuration();
		conf.set("Reviewer2", Reviewer2Serialization);
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
	
	public static Reviewer2 generateBaseReviewer2(String id) throws IOException
	{		
		Reviewer2 Reviewer2 = new Reviewer2(id);
		File data = new File("Data/processedReview");
		BufferedReader br = new BufferedReader(new FileReader(data));
		String line;
		
		while((line = br.readLine()) != null)
		{
			String[] tokens = line.split(",");
			if(tokens[0].equals(Reviewer2.id))
			{
				Reviewer2.ratings.put(tokens[1], Float.parseFloat(tokens[2]));
				System.out.println(tokens[1]);
			}
		}
		return Reviewer2;
	}
}

class Reviewer2 
{
	public String id;
	public HashMap<String, Float> ratings;
	public float ratingMeans = 0;
	
	public Reviewer2(String id)
	{
		this.id = id;
		this.ratings = new HashMap<String, Float>();
	}
	
	public void calculateRatingMean()
	{
		float temp = 0;
		for(String k : ratings.keySet())
		{
			temp += ratings.get(k);
		}
		ratingMeans = temp/ratings.size(); 
	}	
}
