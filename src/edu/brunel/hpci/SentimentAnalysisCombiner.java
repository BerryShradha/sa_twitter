package edu.brunel.hpci;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SentimentAnalysisCombiner extends Reducer<TwitterInfoBean, IntWritable, TwitterInfoBean, IntWritable> {

	/**
	 * Combiner to aggregate tweet count for each node.
	 */
	@Override
	public void reduce(TwitterInfoBean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int scoreCount = 0;
		for (IntWritable value: values) {
			scoreCount += Integer.parseInt(value.toString());
		}
		System.out.println("COMBINER::Input key: " + key + " Input value: " + values + " Output key: " + key.toString() + " Output value: " + scoreCount);
		context.write(key, new IntWritable(scoreCount));
	}
}
