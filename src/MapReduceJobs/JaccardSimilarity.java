package MapReduceJobs;

import java.awt.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JaccardSimilarity {

	public static class User
	{
		public static String interestedUserId = "";
		public static HashSet<String> intestedUserBooks = new HashSet<String>();
		public static final int TRESHOLD = 10;
		public static String smallestUserId;
		public double jaccardRating;
		public ArrayList<String> books;

		public User(double jr, ArrayList<String> books)
		{
			jaccardRating = jr;
			this.books = books;	
		}

		public static void assignSmallestUser(HashMap<String,User> users)
		{
			double smallest = 1;

			for(String userId : users.keySet())
			{
				User temp = users.get(userId);
				if(temp.jaccardRating <= smallest)
				{
					smallest = temp.jaccardRating;
					User.smallestUserId = userId;
				}
			}
		}
	}

	
	public static class JaccardAllPairsMapper extends Mapper<Object, Text, Text, Text> 
	{
		private static double ratingThreshold = 0;
		
		public void map(Object key, Text values, Context context) throws IOException, InterruptedException 
		{
			String[] st = values.toString().split(",");
			String reviewer = st[0];
			String book = st[1];
			double rating = Double.parseDouble(st[2]);

			if(reviewer == User.interestedUserId)
			{
				User.intestedUserBooks.add(book);
			}
			else
			{
				Text emitKey = new Text();
				emitKey.set(reviewer);
				
				Text emitValue = new Text();
				emitValue.set(book);
				
				context.write(emitKey,emitValue);
			}
		}
	}
	
	public static class JaccardTop10Users extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

		private HashMap<String,User> topXUsers = new HashMap<String,User>();

				
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			String userId = key.toString();
			double numerator=0;
			double denominator=0;
			double jaccardCoeff =0;
			HashSet<String> union = new HashSet<String>();
			union.addAll(User.intestedUserBooks);
			ArrayList<String> userXbooks = new ArrayList<String>();
			for(Text t : values)
			{
				String s = t.toString();
				union.add(s);
				userXbooks.add(s);
				if(User.intestedUserBooks.contains(s))
				{
					numerator++;
				}
			}

			denominator = union.size();
			jaccardCoeff = numerator / denominator;

			if(jaccardCoeff > topXUsers.get(User.smallestUserId).jaccardRating)
			{
				topXUsers.put(userId, new User(jaccardCoeff,userXbooks));
				if(topXUsers.size() > User.TRESHOLD)
				{
					topXUsers.remove(User.smallestUserId);	
					User.assignSmallestUser(topXUsers);
				}
			}
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Jaccard Similarity");
		job.setJarByClass(JaccardSimilarity.class);
		job.setMapperClass(JaccardAllPairsMapper.class);
		job.setReducerClass(JaccardTop10Users.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//String fileInput = args[0];
		//if(fileInput == null)
		//{
			String fileInput = "file:///home//epar//workspace//AmazonBookReview//Data//processedReview";
		//}
		
		//String fileOutput = args[1];
		//if(fileOutput == null)
		//{
		String 	fileOutput = "file:///home//epar//workspace//AmazonBookReview//Data//Output";
		//}
		
		FileInputFormat.addInputPath(job, new Path(fileInput));
		FileOutputFormat.setOutputPath(job, new Path(fileOutput));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
