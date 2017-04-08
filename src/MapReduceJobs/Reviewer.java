package MapReduceJobs;

import java.util.HashMap;


public class Reviewer 
{
	public String id;
	public HashMap<String, Float> ratings;
	public float ratingMeans = 0;

	public Reviewer(String id)
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
