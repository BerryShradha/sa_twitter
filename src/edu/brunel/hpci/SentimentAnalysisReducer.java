package edu.brunel.hpci;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SentimentAnalysisReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	/**
	 * Reducer to aggregate tweet count for each sentiment score.
	 */
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int scoreCount = 0;
		for (IntWritable value: values) {
			scoreCount += Integer.parseInt(value.toString());
		}
		context.write(key, new IntWritable(scoreCount));
	}
	
	//@Override
	/*public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		//int scoreCount = 0;
		for (IntWritable value: values) {
			context.write(key, value);
		}
		
	}*/
}
