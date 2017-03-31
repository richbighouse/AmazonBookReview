package JsonParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class PreProcesser{

	private final static String ExtractedFilePath = "Data/subFile";
	private final static String PreprocessedReview = "Data/processedReview";
	private static File data = new File("Data/Books_5.json");
	
	public static int countNumberLines() throws IOException
	{
		int numLines = 0;
		BufferedReader br = new BufferedReader(new FileReader(PreprocessedReview));
		while((br.readLine()) != null)
		{
			numLines ++;
		}
		return numLines;		
	}
	
	public static int preProcessData() throws JsonSyntaxException, IOException
	{
		BufferedReader br = new BufferedReader(new FileReader(data));
		String line = "";
		Review review;
		Gson gson = new Gson();
		PrintWriter writer = new PrintWriter(PreprocessedReview, "UTF-8");
		int numLines = 0 ;
		while((line = br.readLine()) != null)
		{
			review = gson.fromJson(line, Review.class);
			writer.print(review.reviewerID +",");
			writer.print(review.asin+",");
			review.getRating();
			writer.print(review.rating+"\n");	
			numLines++;
		}	
		writer.close();
		return numLines;
	}
	
	public static void extractFirstLines(int numLines) throws IOException
	{
		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new FileReader(data));

		@SuppressWarnings("resource")
		PrintWriter writer = new PrintWriter(ExtractedFilePath, "UTF-8");
		for(int i =0; i<numLines;i++)
		{
			writer.println(br.readLine());
		}
		writer.close();
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException 
	{
		PreProcesser.extractFirstLines(10);
		System.out.println("# of lines in Data: " + PreProcesser.preProcessData());
		System.out.println("# of lines in preprocessed: " + PreProcesser.countNumberLines());
		System.out.println("Job done");
	}

}

