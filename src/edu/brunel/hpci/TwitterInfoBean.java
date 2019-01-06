package edu.brunel.hpci;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TwitterInfoBean implements WritableComparable<TwitterInfoBean>{

	private Text tweetDesc;
	private Text dateCreated;
	private IntWritable sentimentScore;
	
	//Default constructor
	public TwitterInfoBean() {
		super();
		this.tweetDesc = new Text();
		this.dateCreated = new Text();
		this.sentimentScore =new IntWritable();
	}

	//Parameterised constructor
	public TwitterInfoBean(Text tweetDesc, Text dateCreated, IntWritable sentimentScore) {
		super();
		this.tweetDesc = tweetDesc;
		this.dateCreated = dateCreated;
		this.sentimentScore = sentimentScore;
	}

	@Override
	public String toString() {
		return "TwitterInfoBean [tweetDesc=" + tweetDesc + ", dateCreated=" + dateCreated + ", sentimentScore="
				+ sentimentScore + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dateCreated == null) ? 0 : dateCreated.hashCode());
		result = prime * result + ((sentimentScore == null) ? 0 : sentimentScore.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TwitterInfoBean other = (TwitterInfoBean) obj;
		if (dateCreated == null) {
			if (other.dateCreated != null)
				return false;
		} else if (!dateCreated.equals(other.dateCreated))
			return false;
		if (sentimentScore == null) {
			if (other.sentimentScore != null)
				return false;
		} else if (!sentimentScore.equals(other.sentimentScore))
			return false;
		return true;
	}
	
	public Text getTweetDesc() {
		return tweetDesc;
	}

	public void setTweetDesc(Text tweetDesc) {
		this.tweetDesc = tweetDesc;
	}

	public Text getDateCreated() {
		return dateCreated;
	}

	public void setDateCreated(Text dateCreated) {
		this.dateCreated = dateCreated;
	}

	public IntWritable getSentimentScore() {
		return sentimentScore;
	}

	public void setSentimentScore(IntWritable sentimentScore) {
		this.sentimentScore = sentimentScore;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		tweetDesc.readFields(input);
		dateCreated.readFields(input);
		sentimentScore.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		tweetDesc.write(output);
		dateCreated.write(output);
		sentimentScore.write(output);
	}

	/**
	 * Comparable implementation to sort by dateCreated, sentimentScore
	 */
	@Override
	public int compareTo(TwitterInfoBean inputBean) {
		if (dateCreated.compareTo(inputBean.dateCreated) == 0)
			return (sentimentScore.compareTo(inputBean.sentimentScore));
		return (dateCreated.compareTo(inputBean.dateCreated)); 
	}
}
