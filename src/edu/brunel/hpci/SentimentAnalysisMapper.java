package edu.brunel.hpci;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SentimentAnalysisMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	//private Long tweetId;
	private Integer score;
	private IntWritable one = new IntWritable(1);

	/**
	 * Original Mapper
	 * @param key
	 * @param value
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	
		String[] line = value.toString().split(",");
		System.out.println("Size of line is :::::" + line.length);
		//tweetId = Long.parseLong(line[7]);
		boolean isRetweet = false;
		String tweet = line[0]; 
		//String tweet = value.toString();
		List<String> words = new ArrayList<String>();
		words = Arrays.asList(tweet.split(" "));
		score = 0;
		isRetweet = Boolean.valueOf(line[12]);
		if (!isRetweet && !tweet.isEmpty()) {
			for (String eachWord : words) {
				if (SentimentAnalysisMain.sentiments.containsKey(eachWord)) {
					score += SentimentAnalysisMain.sentiments.get(eachWord);
				}
			}
			System.out.println("Tweet is: " + tweet);
			//if (score != 0) {
			IntWritable writableScore = new IntWritable(score);
			context.write(writableScore, one);
			//}
		}

	}

	/**
	 * Mapper to view tweets
	 */
	//@Override
	/*public void map_view(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	
		String tweet = value.toString();
		List<String> words = new ArrayList<String>();
		words = Arrays.asList(tweet.split(" "));
		score = 0;
		for (String eachWord : words) {
			if (SentimentAnalysisMain.sentiments.containsKey(eachWord)) {
				score += SentimentAnalysisMain.sentiments.get(eachWord);
			}
		}
		System.out.println("Tweet is: " + tweet);
		//if (score != 0) {
		IntWritable writableScore = new IntWritable(score);
		context.write(new Text(tweet), writableScore);
		//}
	}*/

	/*private HashMap<Long, String> CreateTweetMap(String URI) throws IOException {
		CSVReader reader = new CSVReader(new FileReader(URI));
		String [] nextLine;
		HashMap<Long, String> tweets = new HashMap<Long, String>();
		Long id;
		String tweet;
		while ((nextLine = reader.readNext()) != null) {
			id = Long.parseLong(nextLine[7]);
			tweet = nextLine[0];
			tweets.put(id, tweet);
		}
		reader.close();
		return tweets;
	}*/
}
