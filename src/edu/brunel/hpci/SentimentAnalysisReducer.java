package edu.brunel.hpci;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SentimentAnalysisReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		double maxTemperature = 0;
		for (DoubleWritable value: values) {
			maxTemperature = Math.max(maxTemperature,  value.get());
		}
		context.write(key, new DoubleWritable(maxTemperature));
	}
}
