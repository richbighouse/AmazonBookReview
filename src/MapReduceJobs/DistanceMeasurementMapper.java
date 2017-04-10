package MapReduceJobs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

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

import JsonParser.TestSet;

import com.google.gson.Gson;

public class DistanceMeasurementMapper {

	public static Reviewer baseReviewer;
	public static Threshold threshold;

	public static class AllPairsMapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text values, Context context) throws IOException, InterruptedException 
		{
			String[] tokens = values.toString().split(",");
			//Writing user, book;rating

			if(!tokens[0].equals(baseReviewer.id)){

				if(threshold.smallerThan)
				{
					if(Double.parseDouble(tokens[2].toString()) <= threshold.threshold)
					{
						context.write(new Text(tokens[0]), new Text(tokens[1] +";"+tokens[2]));
					}
				}
				else
				{
					if(Double.parseDouble(tokens[2].toString()) >= threshold.threshold)
					{
						context.write(new Text(tokens[0]), new Text(tokens[1] +";"+tokens[2]));
					}
				}
			}
		}
	}

	public static class AllPairsReducer extends Reducer<Text, Text, Text, DoubleWritable> 
	{
		private HashMap<Reviewer, Double> similarityMap = new HashMap<Reviewer, Double>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			Reviewer comparedReviewer = new Reviewer(key.toString());			
			HashSet<String> allBooks = new HashSet<String>();//Set of all the combine books of the base Reviewer and compared Reviewer
			allBooks.addAll(baseReviewer.ratings.keySet()); 

			String[] br = new String[2]; //temporary array to split books and ratings
			double rating;
			for(Text t : values)
			{
				br = t.toString().split(";");
				rating = Double.parseDouble(br[1]);
				comparedReviewer.ratings.put(br[0], rating);
				comparedReviewer.ratingMeans += rating;
				allBooks.add(br[0]);
			}
			comparedReviewer.ratingMeans = comparedReviewer.ratingMeans/comparedReviewer.ratings.size();
			double similarity = Reviewer.calculateSimilarity(baseReviewer, comparedReviewer,allBooks);
			similarityMap.put(comparedReviewer, similarity);			
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

					Set<Entry<Reviewer, Double>> entries = similarityMap.entrySet();
					ArrayList<Entry<Reviewer, Double>> listOfEntries =  new ArrayList<Entry<Reviewer, Double>>(entries);
					Collections.sort(listOfEntries, valueComparator);

					int end = (listOfEntries.size() > Reviewer.topXUser)? Reviewer.topXUser:listOfEntries.size();
					
					for(int i = 0; i < end; i++)
					{
						Entry<Reviewer, Double> entry = listOfEntries.get(i);
						String id = entry.getKey().id;
						Double cosin = entry.getValue();
						cosin = Math.round(cosin*10000d)/10000d;

						context.write(new Text(id), new DoubleWritable(cosin));
					}
		}
	}

	public static void Execute (String fileInput, String fileOutput, Threshold thres, Reviewer reviewer) throws IOException, ClassNotFoundException, InterruptedException
	{
		DistanceMeasurementMapper.threshold = thres;
		baseReviewer = reviewer;
		baseReviewer.ratings = TestSet.getSetWithThreshold(thres,reviewer.ratings);
		
		File outputFolder = new File(fileOutput.replace("file:/",""));

		if(outputFolder.exists())
		{
			FileUtils.deleteDirectory(outputFolder);
		}

		Gson gson = new Gson();
		String ReviewerSerialization = gson.toJson(baseReviewer); //Serialize to pass to Reduce jobs	

		Configuration conf = new Configuration();
		conf.set("Reviewer", ReviewerSerialization);
		Job job = Job.getInstance(conf, "Distance Similarity");
		job.setJarByClass(DistanceMeasurementMapper.class);
		job.setMapperClass(AllPairsMapper.class);
		job.setReducerClass(AllPairsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(fileInput));
		FileOutputFormat.setOutputPath(job, new Path(fileOutput));	
		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException 
	{	
		//Task: PEARSON, COSINE, JACCARD
		Reviewer.Task task = Reviewer.Task.COSINE;

		//String fileInput = "file:///home//epar//workspace//AmazonBookReview//Data//Sample";
		//String 	fileOutput = "file:///home//epar//workspace//AmazonBookReview//Data//Output";
		String fileInput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Data//Sample";
		String 	fileOutput = "file:///home//rich//dev//workspaces//java8//AmazonBookReview//Output";
		File outputFolder = new File(fileOutput.replace("file:/",""));

		if(outputFolder.exists())
		{
			FileUtils.deleteDirectory(outputFolder);
		}	



		Reviewer.DataPath = fileInput.substring(fileInput.indexOf("AmazonBookReview")+"AmazonBookReview//".length());//Used to find all the books of a given user
		Reviewer.topXUser = 4;//Used in the cleanup to output the top X users	
		baseReviewer = Reviewer.generateBaseReviewer("user1",task);//Creates our interested user

		Gson gson = new Gson();
		String ReviewerSerialization = gson.toJson(baseReviewer); //Serialize to pass to Reduce jobs	

		Configuration conf = new Configuration();
		conf.set("Reviewer", ReviewerSerialization);
		Job job = Job.getInstance(conf, "Distance Similarity");
		job.setJarByClass(DistanceMeasurementMapper.class);
		job.setMapperClass(AllPairsMapper.class);
		job.setReducerClass(AllPairsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(fileInput));
		FileOutputFormat.setOutputPath(job, new Path(fileOutput));
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}


}

