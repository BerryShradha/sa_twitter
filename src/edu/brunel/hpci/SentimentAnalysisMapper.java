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
		List<String> line = new ArrayList<String>(Arrays.asList(value.toString().split(",",-1)));
			logger.info("Size of row is: " + line.size());
		//int count = 1;
		score = 0;
		/*for (String s : line) {
				logger.info("Line#" + count+ "Text: " + s);
			count++;
		}*/
		String tweet = cleanTweet(line); //Call function to perform cleanup
		logger.info("Tweet is: " + tweet + " :: Empty: " + tweet.isEmpty());
		if (line.get(0).startsWith("\"text\"") || line.get(0).startsWith("text"))
			return;
		//Check for phrases from dictionary
		for (String eachPhrase : SentimentAnalysisMain.sentimentPhrases.keySet()) {
			if (tweet.trim().contains(eachPhrase)) {
				logger.info("Phrase matched: " + eachPhrase);
				score += SentimentAnalysisMain.sentimentPhrases.get(eachPhrase);
				tweet = tweet.replace(eachPhrase, "");
			}
		}
		//Check for words from dictionary
		List<String> words = new ArrayList<String>();
		words = Arrays.asList(tweet.trim().split(" "));
		if (!tweet.trim().isEmpty()) {
			for (String eachWord : words) {
				eachWord = eachWord.toLowerCase();
				if (SentimentAnalysisMain.sentimentWords.containsKey(eachWord)) {
						logger.info("Word matched: " + eachWord);
					score += SentimentAnalysisMain.sentimentWords.get(eachWord);
				}
			}

			logger.info("Tweet is: " + tweet + " Score is: " + score);
			IntWritable writableScore = new IntWritable(score);
			context.write(writableScore, one);
		}

	}

	/**
	 * Clean tweet to return description 
	 */
	public String cleanTweet(List<String> tweetRow) {
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
		if (valid && tweetRow.get(size - RETWEET_COLUMN).equals("TRUE")) //Remove retweets
			valid = false; 
		if (valid) {
			List<String> desc = tweetRow.subList(0, size - (DEFAULT_COLUMNS - 1)); //Find tweet desc
			Iterator<String> iter = desc.listIterator();
			while (iter.hasNext())
				cleanedTweet = cleanedTweet.concat(iter.next());
			cleanedTweet = removeSpclChars(cleanedTweet); //Removes any URLs, hashtags, references in the tweet
			cleanedTweet = cleanedTweet.replaceAll("[\\’\\!\\-\\+\\.\\^:,\"']"," ");
			cleanedTweet.trim();
		}
		return cleanedTweet;
	}

	/**
	 * Removes URLs, hashtags, references from tweets
	 * @param tweet
	 * @return Modified tweet
	 */
	String removeSpclChars(String tweet) {
		String newTweet = tweet;
		//Remove URLs
		final Pattern urlPattern = Pattern.compile(
				"(?:^|[\\W])((ht|f)tp(s?):\\/\\/|www\\.)"
						+ "(([\\w\\-]+\\.){1,}?([\\w\\-.~]+\\/?)*"
						+ "[\\p{Alnum}.,%_=?&#\\-+()\\[\\]\\*$~@!:/{};']*)",
						Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
		Matcher matcher = urlPattern.matcher(tweet);
		while (matcher.find())
			newTweet = matcher.replaceAll("");
		//Remove hashtags and references
		for (String eachWord : newTweet.split("\\s+")) {
			if (eachWord.startsWith("#") || eachWord.startsWith("@")) {
				newTweet = newTweet.replace(eachWord, "");
			}
		}
		return newTweet;
	}
}
