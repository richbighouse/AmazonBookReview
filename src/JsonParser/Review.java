package JsonParser;

public class Review {

	public String reviewerID;
	public String asin;
	public String reviewerName;
	public int[] helpful;
	public String reviewText;
	public String overall;
	public String summary;
	public long unixReviewTime;
	public String reviewTiem;
	public float rating;
	
	public String toString(){
		return this.reviewText;
	}
	
	public void getRating(){
		this.rating = Float.parseFloat(this.overall);
	}
	
}
