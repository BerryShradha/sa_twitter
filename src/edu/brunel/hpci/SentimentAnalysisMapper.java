package edu.brunel.hpci;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class SentimentAnalysisMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	private Integer score;
	private IntWritable one = new IntWritable(1);
	private static final Logger logger = Logger.getLogger(SentimentAnalysisMapper.class);

	/**
	 * Mapper to calculate score of each tweet
	 * @param key
	 * @param value
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	
		if (value.toString().startsWith("\"text\",\"favorited\",\"favoriteCount\""))
			return;
		List<String> line = new ArrayList<String>(Arrays.asList(value.toString().split(",",-1)));
		logger.info("Size of row is: " + line.size());
		int count = 1;
		for (String s : line) {
			logger.info("Line#" + count+ "Text: " + s);
			count++;
		}
		String tweet = cleanTweet(line); //Call function to perform cleanup
		logger.info("Tweet is: " + tweet + " :: Empty: " + tweet.isEmpty());
		if (tweet.isEmpty()) 
			return;
		List<String> words = new ArrayList<String>();
		words = Arrays.asList(tweet.split(" "));
		score = 0;
		if (!tweet.isEmpty()) {
			for (String eachWord : words) {
				if (SentimentAnalysisMain.sentiments.containsKey(eachWord)) {
					score += SentimentAnalysisMain.sentiments.get(eachWord);
				}
			}
			IntWritable writableScore = new IntWritable(score);
			context.write(writableScore, one);
		}

	}

	/**
	 * Clean tweet to return description 
	 */
	public String cleanTweet(List<String> tweetRow) {
		logger.info("Iput to CleanTweet: " + tweetRow);
		boolean valid = true;
		final int DEFAULT_COLUMNS = 16;
		final int RETWEET_COLUMN = 4;
		String cleanedTweet = "";
		int size = tweetRow.size(); 
		if (valid && tweetRow.isEmpty()) //Empty lines
			valid = false;
		if (valid && tweetRow.size() < 16) //Ensure minimum columns present
			valid = false;
		if (valid)
			logger.info("Retweet Indicator: " + tweetRow.get(size - RETWEET_COLUMN));
		if (valid && tweetRow.get(size - RETWEET_COLUMN).equals("TRUE")) //Remove retweets
			valid = false; 
		if (valid) {
			List<String> desc = tweetRow.subList(0, size - (DEFAULT_COLUMNS - 1)); //Find tweet desc
			Iterator<String> iter = desc.listIterator();
			while (iter.hasNext())
				cleanedTweet = cleanedTweet.concat(iter.next());
			cleanedTweet = removeURL(cleanedTweet); //Removes any URLs in the tweet description
			cleanedTweet = cleanedTweet.replaceAll("[\\-\\+\\.\\^:,\"']","");
			cleanedTweet.trim();
		}
		logger.info("Output from CleanTweet: " + cleanedTweet);
		return cleanedTweet;
	}
	
	/**
	 * Removes URLs from tweets
	 * @param tweet
	 * @return MOdified tweet without any URLs
	 */
	String removeURL(String tweet) {
		String newTweet = tweet;
		final Pattern urlPattern = Pattern.compile(
		        "(?:^|[\\W])((ht|f)tp(s?):\\/\\/|www\\.)"
		                + "(([\\w\\-]+\\.){1,}?([\\w\\-.~]+\\/?)*"
		                + "[\\p{Alnum}.,%_=?&#\\-+()\\[\\]\\*$~@!:/{};']*)",
		        Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
		Matcher matcher = urlPattern.matcher(tweet);
		while (matcher.find())
			newTweet = matcher.replaceAll("");
		return newTweet;
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
