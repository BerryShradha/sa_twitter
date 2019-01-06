package edu.brunel.hpci;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SentimentAnalysisReducer extends Reducer<TwitterInfoBean, IntWritable, Text, IntWritable> {

	/**
	 * Reducer to aggregate tweet count for each sentiment score.
	 */
	@Override
	public void reduce(TwitterInfoBean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int scoreCount = 0;
		Text customKey = new Text(key.getDateCreated() + "::" + key.getSentimentScore());
		for (IntWritable value: values) {
			scoreCount += Integer.parseInt(value.toString());
		}
		System.out.println("REDUCER::Input key: " + key + " Input value: " + values + " Output key: " + customKey.toString() + " Output value: " + scoreCount);
		context.write(customKey, new IntWritable(scoreCount));
	}
}
