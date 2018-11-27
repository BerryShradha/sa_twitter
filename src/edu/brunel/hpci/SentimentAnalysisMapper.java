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

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	
		String[] line = value.toString().split(",");
		//tweetId = Long.parseLong(line[7]);
		String tweet = line[0];
		List<String> words = new ArrayList<String>();
		words = Arrays.asList(tweet.split(" "));
		score = 0;
		for (String eachWord : words) {
			if (SentimentAnalysisMain.sentiments.containsKey(eachWord)) {
				score += SentimentAnalysisMain.sentiments.get(eachWord);
			}
		}
		if (score != 0) {
			IntWritable writableScore = new IntWritable(score);
			context.write(writableScore, one);
		}
	}

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
